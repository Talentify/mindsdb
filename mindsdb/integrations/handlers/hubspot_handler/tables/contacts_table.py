from typing import List, Dict, Text, Any
import pandas as pd
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectId as HubSpotBatchObjectIdInput,
    BatchInputSimplePublicObjectBatchInput as HubSpotBatchObjectBatchInput,
    BatchInputSimplePublicObjectInputForCreate as HubSpotBatchObjectInputCreate,
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


logger = log.getLogger(__name__)


class ContactsTable(APITable):
    """Hubspot Contacts table."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Contacts data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Contacts matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "contacts",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            contacts_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        contacts_df = select_statement_executor.execute_query()

        return contacts_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/contacts/batch/create" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['email', 'firstname', 'firstname', 'phone', 'company', 'website'],
            mandatory_columns=['email'],
            all_mandatory=False,
        )
        contact_data = insert_statement_parser.parse_query()
        self.create_contacts(contact_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/contacts/batch/update" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        update_query_executor = UPDATEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = update_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.update_contacts(contact_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/contacts/batch/archive" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        delete_query_executor = DELETEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = delete_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.delete_contacts(contact_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_contacts(limit=1)).columns.tolist()

    def get_contacts(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        contacts = hubspot.crm.contacts.get_all(**kwargs)
        contacts_dict = [
            {
                "id": contact.id,
                "email": contact.properties["email"],
                "firstname": contact.properties.get("firstname", None),
                "lastname": contact.properties.get("lastname", None),
                "phone": contact.properties.get("phone", None),
                "company": contact.properties.get("company", None),
                "website": contact.properties.get("website", None),
                "createdate": contact.properties["createdate"],
                "lastmodifieddate": contact.properties["lastmodifieddate"],
            }
            for contact in contacts
        ]
        return contacts_dict

    def create_contacts(self, contacts_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        contacts_to_create = [HubSpotObjectInputCreate(properties=contact) for contact in contacts_data]
        try:
            created_contacts = hubspot.crm.contacts.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=contacts_to_create)
            )
            logger.info(f"Contacts created with ID {[created_contact.id for created_contact in created_contacts.results]}")
        except Exception as e:
            raise Exception(f"Contacts creation failed {e}")

    def update_contacts(self, contact_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [HubSpotObjectBatchInput(id=contact_id, properties=values_to_update) for contact_id in contact_ids]
        try:
            updated_contacts = hubspot.crm.contacts.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=contacts_to_update),
            )
            logger.info(f"Contacts with ID {[updated_contact.id for updated_contact in updated_contacts.results]} updated")
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        contacts_to_delete = [HubSpotObjectId(id=contact_id) for contact_id in contact_ids]
        try:
            hubspot.crm.contacts.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=contacts_to_delete),
            )
            logger.info("Contacts deleted")
        except Exception as e:
            raise Exception(f"Contacts deletion failed {e}")