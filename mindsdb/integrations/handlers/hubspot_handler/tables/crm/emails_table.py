from typing import List, Dict, Text, Any
import pandas as pd
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectId as HubSpotBatchObjectIdInput,
    BatchInputSimplePublicObjectBatchInput as HubSpotBatchObjectBatchInput,
    BatchInputSimplePublicObjectBatchInputForCreate as HubSpotBatchObjectInputCreate,
)

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import (
    INSERTQueryParser,
    SELECTQueryParser,
    UPDATEQueryParser,
    DELETEQueryParser,
    SELECTQueryExecutor,
    UPDATEQueryExecutor,
    DELETEQueryExecutor,
)
from mindsdb.utilities import log
from mindsdb.integrations.handlers.hubspot_handler.tables.crm.base_hubspot_table import HubSpotSearchMixin

logger = log.getLogger(__name__)


class EmailsTable(HubSpotSearchMixin, APITable):
    """Hubspot Emails table (Activity)."""

    DEFAULT_PROPERTIES = [
        'hs_timestamp', 'hs_email_subject', 'hs_email_text', 'hs_email_html',
        'hs_email_direction', 'hs_email_status', 'hs_email_from', 'hs_email_to',
        'hubspot_owner_id', 'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Emails data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "emails",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        requested_properties = None
        if selected_columns and len(selected_columns) > 0:
            requested_properties = [col for col in selected_columns if col != 'id']

        if where_conditions and len(where_conditions) > 0:
            hubspot_filters = self._build_search_filters(where_conditions)
            if hubspot_filters:
                logger.info(f"Using HubSpot search API with {len(hubspot_filters)} filter(s)")
                emails_df = pd.json_normalize(
                    self.search_emails(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                emails_df = pd.json_normalize(
                    self.get_emails(limit=result_limit, properties=requested_properties)
                )
        else:
            emails_df = pd.json_normalize(
                self.get_emails(limit=result_limit, properties=requested_properties)
            )

        # Filter selected_columns to only include columns that actually exist in the dataframe
        # This handles cases where requested properties don't exist in HubSpot
        if not emails_df.empty and selected_columns:
            available_columns = [col for col in selected_columns if col in emails_df.columns]
            if len(available_columns) < len(selected_columns):
                missing = set(selected_columns) - set(available_columns)
                logger.warning(f"Some requested columns not available in emails data: {missing}")
            selected_columns = available_columns

        select_statement_executor = SELECTQueryExecutor(
            emails_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Emails"""
        try:
            properties_cache = self.handler.get_properties_cache('emails')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['hs_timestamp', 'hs_email_subject', 'hs_email_text']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['hs_timestamp'],
            all_mandatory=False,
        )
        emails_data = insert_statement_parser.parse_query()
        self.create_emails(emails_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Emails"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        emails_df = pd.json_normalize(self.get_emails())
        update_query_executor = UPDATEQueryExecutor(emails_df, where_conditions)
        emails_df = update_query_executor.execute_query()
        email_ids = emails_df['id'].tolist()
        self.update_emails(email_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Emails"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        emails_df = pd.json_normalize(self.get_emails())
        delete_query_executor = DELETEQueryExecutor(emails_df, where_conditions)
        emails_df = delete_query_executor.execute_query()
        email_ids = emails_df['id'].tolist()
        self.delete_emails(email_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_emails(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch emails with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('emails')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch
        response = hubspot.crm.objects.basic_api.get_page(object_type="emails", **kwargs)

        emails_dict = []
        for email in response.results:
            email_dict = {"id": email.id}
            if hasattr(email, 'properties') and email.properties:
                for prop_name, prop_value in email.properties.items():
                    email_dict[prop_name] = prop_value
            emails_dict.append(email_dict)

        return emails_dict

    def search_emails(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search emails using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('emails')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_emails = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(object_type="emails", 
                    public_object_search_request=search_request
                )

                for email in response.results:
                    email_dict = {"id": email.id}
                    if hasattr(email, 'properties') and email.properties:
                        for prop_name, prop_value in email.properties.items():
                            email_dict[prop_name] = prop_value
                    all_emails.append(email_dict)

                if limit and len(all_emails) >= limit:
                    all_emails = all_emails[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching emails: {e}")
            raise Exception(f"Email search failed: {e}")

        logger.info(f"Found {len(all_emails)} emails matching filters")
        return all_emails

    def create_emails(self, emails_data: List[Dict[Text, Any]]) -> None:
        """Create emails"""
        hubspot = self.handler.connect()
        emails_to_create = [HubSpotObjectInputCreate(properties=email) for email in emails_data]
        try:
            created_emails = hubspot.crm.objects.batch_api.create(object_type="emails", 
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=emails_to_create)
            )
            logger.info(f"Emails created with IDs {[email.id for email in created_emails.results]}")
        except Exception as e:
            raise Exception(f"Emails creation failed: {e}")

    def update_emails(self, email_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update emails"""
        hubspot = self.handler.connect()
        emails_to_update = [HubSpotObjectBatchInput(id=email_id, properties=values_to_update) for email_id in email_ids]
        try:
            updated_emails = hubspot.crm.objects.batch_api.update(object_type="emails", 
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=emails_to_update)
            )
            logger.info(f"Emails with IDs {[email.id for email in updated_emails.results]} updated")
        except Exception as e:
            raise Exception(f"Emails update failed: {e}")

    def delete_emails(self, email_ids: List[Text]) -> None:
        """Delete emails"""
        hubspot = self.handler.connect()
        emails_to_delete = [HubSpotObjectId(id=email_id) for email_id in email_ids]
        try:
            hubspot.crm.objects.batch_api.archive(object_type="emails", 
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=emails_to_delete)
            )
            logger.info("Emails deleted")
        except Exception as e:
            raise Exception(f"Emails deletion failed: {e}")
