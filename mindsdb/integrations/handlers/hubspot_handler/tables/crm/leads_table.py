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


class LeadsTable(HubSpotSearchMixin, APITable):
    """Hubspot Leads table (Sales Hub Professional and Enterprise)."""

    DEFAULT_PROPERTIES = [
        'firstname', 'lastname', 'email', 'phone', 'company', 'website',
        'jobtitle', 'hs_lead_status', 'lifecyclestage', 'hubspot_owner_id',
        'createdate', 'hs_lastmodifieddate'
    ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls Hubspot Leads data"""
        select_statement_parser = SELECTQueryParser(
            query,
            "leads",
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
                leads_df = pd.json_normalize(
                    self.search_leads(
                        filters=hubspot_filters,
                        properties=requested_properties,
                        limit=result_limit
                    )
                )
                where_conditions = []
            else:
                logger.info("No valid HubSpot filters, using get_all")
                leads_df = pd.json_normalize(
                    self.get_leads(limit=result_limit, properties=requested_properties)
                )
        else:
            leads_df = pd.json_normalize(
                self.get_leads(limit=result_limit, properties=requested_properties)
            )

        select_statement_executor = SELECTQueryExecutor(
            leads_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into HubSpot Leads"""
        try:
            properties_cache = self.handler.get_properties_cache('leads')
            supported_columns = list(properties_cache['property_names'])
        except Exception as e:
            logger.warning(f"Failed to get dynamic columns for insert: {e}")
            supported_columns = ['firstname', 'lastname', 'email', 'phone', 'company']

        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=supported_columns,
            mandatory_columns=['email'],
            all_mandatory=False,
        )
        leads_data = insert_statement_parser.parse_query()
        self.create_leads(leads_data)

    def update(self, query: ast.Update) -> None:
        """Updates HubSpot Leads"""
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        leads_df = pd.json_normalize(self.get_leads())
        update_query_executor = UPDATEQueryExecutor(leads_df, where_conditions)
        leads_df = update_query_executor.execute_query()
        lead_ids = leads_df['id'].tolist()
        self.update_leads(lead_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """Deletes HubSpot Leads"""
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        leads_df = pd.json_normalize(self.get_leads())
        delete_query_executor = DELETEQueryExecutor(leads_df, where_conditions)
        leads_df = delete_query_executor.execute_query()
        lead_ids = leads_df['id'].tolist()
        self.delete_leads(lead_ids)

    def get_columns(self) -> List[Text]:
        """Get column names for the table"""
        return ['id'] + self.DEFAULT_PROPERTIES

    def get_leads(self, properties: List[Text] = None, **kwargs) -> List[Dict]:
        """Fetch leads with specified properties"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('leads')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        kwargs['properties'] = properties_to_fetch

        try:
            # Leads might use different API endpoint depending on HubSpot configuration
            leads = hubspot.crm.objects.basic_api.get_page(
                object_type="leads",
                **kwargs
            )

            leads_dict = []
            for lead in leads.results:
                lead_dict = {"id": lead.id}
                if hasattr(lead, 'properties') and lead.properties:
                    for prop_name, prop_value in lead.properties.items():
                        lead_dict[prop_name] = prop_value
                leads_dict.append(lead_dict)

            return leads_dict
        except Exception as e:
            logger.error(f"Error fetching leads: {e}")
            # Fallback: return empty list if leads object is not available
            logger.warning("Leads object may not be available in this HubSpot account (requires Sales Hub Professional or Enterprise)")
            return []

    def search_leads(self, filters: List[Dict], properties: List[Text] = None, limit: int = None) -> List[Dict]:
        """Search leads using HubSpot search API"""
        hubspot = self.handler.connect()

        if properties is None:
            properties_to_fetch = self.DEFAULT_PROPERTIES
        elif len(properties) == 0:
            properties_cache = self.handler.get_properties_cache('leads')
            properties_to_fetch = list(properties_cache['property_names'])
        else:
            properties_to_fetch = properties

        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties_to_fetch,
            "limit": min(limit or 100, 100),
        }

        all_leads = []
        after = 0

        try:
            while True:
                if after > 0:
                    search_request["after"] = after

                response = hubspot.crm.objects.search_api.do_search(
                    object_type="leads",
                    public_object_search_request=search_request
                )

                for lead in response.results:
                    lead_dict = {"id": lead.id}
                    if hasattr(lead, 'properties') and lead.properties:
                        for prop_name, prop_value in lead.properties.items():
                            lead_dict[prop_name] = prop_value
                    all_leads.append(lead_dict)

                if limit and len(all_leads) >= limit:
                    all_leads = all_leads[:limit]
                    break

                if not hasattr(response, 'paging') or not response.paging:
                    break

                if hasattr(response.paging, 'next') and response.paging.next:
                    after = response.paging.next.after
                else:
                    break

        except Exception as e:
            logger.error(f"Error searching leads: {e}")
            logger.warning("Leads object may not be available in this HubSpot account (requires Sales Hub Professional or Enterprise)")
            return []

        logger.info(f"Found {len(all_leads)} leads matching filters")
        return all_leads

    def create_leads(self, leads_data: List[Dict[Text, Any]]) -> None:
        """Create leads"""
        hubspot = self.handler.connect()
        leads_to_create = [HubSpotObjectInputCreate(properties=lead) for lead in leads_data]
        try:
            created_leads = hubspot.crm.objects.batch_api.create(
                object_type="leads",
                batch_input_simple_public_object_input_for_create=HubSpotBatchObjectInputCreate(inputs=leads_to_create)
            )
            logger.info(f"Leads created with IDs {[lead.id for lead in created_leads.results]}")
        except Exception as e:
            raise Exception(f"Leads creation failed: {e}")

    def update_leads(self, lead_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        """Update leads"""
        hubspot = self.handler.connect()
        leads_to_update = [HubSpotObjectBatchInput(id=lead_id, properties=values_to_update) for lead_id in lead_ids]
        try:
            updated_leads = hubspot.crm.objects.batch_api.update(
                object_type="leads",
                batch_input_simple_public_object_batch_input=HubSpotBatchObjectBatchInput(inputs=leads_to_update)
            )
            logger.info(f"Leads with IDs {[lead.id for lead in updated_leads.results]} updated")
        except Exception as e:
            raise Exception(f"Leads update failed: {e}")

    def delete_leads(self, lead_ids: List[Text]) -> None:
        """Delete leads"""
        hubspot = self.handler.connect()
        leads_to_delete = [HubSpotObjectId(id=lead_id) for lead_id in lead_ids]
        try:
            hubspot.crm.objects.batch_api.archive(
                object_type="leads",
                batch_input_simple_public_object_id=HubSpotBatchObjectIdInput(inputs=leads_to_delete)
            )
            logger.info("Leads deleted")
        except Exception as e:
            raise Exception(f"Leads deletion failed: {e}")
