from datetime import datetime
from enum import StrEnum
from typing import Annotated, List, Literal, Optional, Type

from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from src.models.constants import OPENMETADATA_SUPPORTED_DATATYPES




class ComponentKind(StrEnum):
    OUTPUTPORT = "outputport"
    WORKLOAD = "workload"
    STORAGE = "storage"
    OBSERVABILITY = "observability"


class TagSourceTagLabel(StrEnum):
    CLASSIFICATION = "Classification"
    GLOSSARY = "Glossary"
    TAG = "Tag"


class LabelTypeTagLabel(StrEnum):
    MANUAL = "Manual"
    PROPAGATED = "Propagated"
    AUTOMATED = "Automated"
    DERIVED = "Derived"


class StateTagLabel(StrEnum):
    SUGGESTED = "Suggested"
    CONFIRMED = "Confirmed"


class OpenMetadataTagLabel(BaseModel):
    tagFQN: Optional[str] = None
    description: Optional[str] = None
    source: Optional[TagSourceTagLabel] = TagSourceTagLabel.CLASSIFICATION
    labelType: Optional[LabelTypeTagLabel] = LabelTypeTagLabel.MANUAL
    state: Optional[StateTagLabel] = StateTagLabel.CONFIRMED
    href: Optional[str] = None


class ConnectionTypeWorkload(StrEnum):
    HOUSEKEEPING = "HouseKeeping"
    DATAPIPELINE = "DataPipeline"

class Masking(BaseModel):
    generatorType: str
    link: Optional[str] = None
    specific: Optional[dict] = None

class OpenMetadataColumn(BaseModel):
    name: str
    dataType: str
    description: Optional[str] = None
    tags: Optional[List[OpenMetadataTagLabel]] = None
    masking: Optional[Masking] = None
    dataLength: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

    @field_validator("dataType")
    @classmethod
    def check_dataType(cls, value, values):
        if value.upper() not in OPENMETADATA_SUPPORTED_DATATYPES:
            raise ValueError(
                'Column "'
                + values["name"]
                + '" specifies dataType of "'
                + value
                + '" but this is not a valid OpenMetadata data type'
            )
        return value


class DataContract(BaseModel):
    schema_: List[OpenMetadataColumn] = Field(..., alias="schema")
    termsAndConditions: Optional[str] = None
    SLA: Optional[dict] = None  

class DataSharingAgreement(BaseModel):
    purpose: Optional[str] = None
    billing: Optional[str] = None
    security: Optional[str] = None
    intendedUsage: Optional[str] = None
    limitations: Optional[str] = None
    lifeCycle: Optional[str] = None
    confidentiality: Optional[str] = None


class InputWorkload(BaseModel):
    outputPortName: Optional[str] = None
    systemName: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def check_mutual_exclusivity(cls, values):
        field1 = values.get("outputPortName")
        field2 = values.get("systemName")

        if field1 is not None and field2 is not None:
            raise ValueError(
                "outputPortName and systemName are mutually exclusive, but both are provided"  # noqa: E501
            )

        return values


class Component(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    name: str
    fullyQualifiedName: Optional[str] = None
    description: str
    specific: dict
    kind: ComponentKind

    

class OutputPort(Component):
    model_config = ConfigDict(extra="allow")
    kind: Literal[ComponentKind.OUTPUTPORT]

    version: str
    infrastructureTemplateId: str
    useCaseTemplateId: Optional[str] = None
    dependsOn: List[str]
    platform: Optional[str] = None
    technology: Optional[str] = None
    outputPortType: str
    creationDate: Optional[datetime] = None
    startDate: Optional[datetime] = None
    dataContract: DataContract
    dataSharingAgreement: DataSharingAgreement
    tags: Optional[List[OpenMetadataTagLabel]] = None
    sampleData: Optional[dict] = None
    semanticLinking: List[dict]

    
    
    @field_validator("kind")
    @classmethod
    def check_kind(cls, value, values):
        if value == ComponentKind.OUTPUTPORT:
            return value
        else:
            raise ValueError(
                f"kind of component with id {values.get('id')} must be 'outputport'"  # noqa: E501
            )

class Workload(Component):
    model_config = ConfigDict(extra="allow")
    kind: Literal[ComponentKind.WORKLOAD]

    version: str
    infrastructureTemplateId: str
    useCaseTemplateId: Optional[str] = None
    dependsOn: List[str]
    platform: Optional[str] = None
    technology: Optional[str] = None
    workloadType: Optional[str] = None
    connectionType: ConnectionTypeWorkload
    tags: List[OpenMetadataTagLabel]
    readsFrom: Optional[List[InputWorkload]] = None



    def __init__(self, **data):
        reads_from_values = data.get("readsFrom", [])
        connection_type = data.get("connectionType")

        input_workloads = []

        # ReadsFrom is filled only for DataPipeline workloads
        if (
            connection_type != ConnectionTypeWorkload.DATAPIPELINE
            and len(reads_from_values) > 0
        ):
            raise ValueError(
                "readsFrom is only allowed when connectionType is 'DATAPIPELINE'"  # noqa: E501
            )
            return None

        # Output Ports are identified with DP_UK:$OutputPortName,
        # while external systems will be defined by a URN in the form urn:dmb:ex:$SystemName    # noqa: E501
        for reads_from_value in reads_from_values:
            if reads_from_value.startswith("DP_UK:"):
                input_workload = InputWorkload(outputPortName=reads_from_value)
            elif reads_from_value.startswith("urn:dmb:ex:"):
                input_workload = InputWorkload(systemName=reads_from_value)
            else:
                raise ValueError(
                    f"Incorrect value in readsFrom: {reads_from_value}. "
                    f"Value should start with DP_UK: or urn:dmb:ex:"
                )

            input_workloads.append(input_workload)

        data["readsFrom"] = input_workloads
        super().__init__(**data)

    @field_validator("kind")
    @classmethod
    def check_kind(cls, value, values):
        if value == ComponentKind.WORKLOAD:
            return value
        else:
            raise ValueError(
                f"kind of component with id {values.get('id')} must be 'workload'"  # noqa: E501
            )



class StorageArea(Component):
    model_config = ConfigDict(extra="allow")
    kind: Literal[ComponentKind.STORAGE]

    # owners: List[str]
    infrastructureTemplateId: str
    useCaseTemplateId: Optional[str] = None
    dependsOn: List[str]
    platform: Optional[str] = None
    technology: Optional[str] = None
    storageType: Optional[str] = None
    tags: List[OpenMetadataTagLabel]

   

    @field_validator("kind")
    @classmethod
    def check_kind(cls, value, values):
        if value == ComponentKind.STORAGE:
            return value
        else:
            raise ValueError(
                f"kind of component with id {values.get('id')} must be 'storage'"  # noqa: E501
            )

class Observability(Component):

    model_config = ConfigDict(extra="allow")
    kind: Literal[ComponentKind.OBSERVABILITY]

    endpoint: str
    completeness: dict
    dataProfiling: dict
    freshness: dict
    availability: dict
    dataQuality: dict

    @field_validator("kind")
    @classmethod
    def check_kind(cls, value, values):
        if value == ComponentKind.OBSERVABILITY:
            return value
        else:
            raise ValueError(
                f"kind of component with id {values.get('id')} must be 'observability'"  # noqa: E501
            )
        
component_map: dict[str, Type[Component]] = {
    ComponentKind.OBSERVABILITY: Observability,
    ComponentKind.OUTPUTPORT: OutputPort,
    ComponentKind.WORKLOAD: Workload,
    ComponentKind.STORAGE: StorageArea,
}


def parse_component(data: dict) -> Component:
    kind = data.get("kind")
    if kind not in component_map:
        raise ValueError(f"Unknown component kind: {kind}")
    component = component_map[kind](**data)
    return component





class DataProduct(BaseModel):
    id: str
    name: str
    fullyQualifiedName: Optional[str] = None
    description: str
    kind: Literal["dataproduct"]
    domain: str
    domainId: str
    version: str
    environment: str
    dataProductOwner: str
    dataProductOwnerDisplayName: Optional[str] = None
    email: Optional[str] = None
    ownerGroup: str
    devGroup: str
    informationSLA: Optional[str] = None
    status: Optional[str] = None
    maturity: Optional[str] = None
    billing: Optional[dict] = None
    tags: List[OpenMetadataTagLabel]
    specific: dict
    components: List[Annotated[Component, BeforeValidator(parse_component)]]

    

    def get_components_by_kind(self, kind: str) -> List[Component]:
        """
        Filters the components associated with the data product and returns
        a list containing only the components that have the specified kind.

        Args:
            kind (str): The kind of components to retrieve.

        Returns:
            List[Component]: A list of Component objects that match the specified kind.
            If no matching components are found, an empty list is returned.

        Example:
            To retrieve all components of kind 'outputport' from a data product 'my_data_product':
            >>> outputport_components = my_data_product.get_components_by_kind('outputport')
        """  # noqa: E501

        new_components_list = [
            component for component in self.components if component.kind == kind
        ]

        return new_components_list

    def get_component_by_id(self, component_id: str) -> Component | None:
        """
        Retrieve a component within the data product by its unique identifier.

        This method searches for a component with the specified ID within the data product's
        list of components and returns the matching component, if found.

        Args:
            component_id (str): The unique identifier of the component to retrieve.

        Returns:
            Component | None: The Component object with the specified ID if found, or None if
            no matching component is found.

        Example:
           To retrieve a specific component with ID '12345' from a data product 'my_data_product':
           >>> specific_component = my_data_product.get_component_by_id('12345')
           >>> if specific_component:
           ...     print(f"Found component: {specific_component.name}")
           ... else:
           ...     print("Component not found.")
        """  # noqa: E501
        for component in self.components:
            if component.id == component_id:
                return component
        return None

    def get_output_ports(self) -> List[OutputPort]:
        """
        Retrieve a list of output ports associated with the data product.

        This method filters the components of kind 'outputport' that are associated with
        the data product and returns a list containing these output ports.

        Returns:
            List[OutputPort]: A list of OutputPort objects that represent the output
            ports associated with the data product.

        Example:
            To retrieve all output ports from a data product 'my_data_product':
            >>> output_ports = my_data_product.get_output_ports()
        """  # noqa: E501

        output_ports: List[OutputPort] = []
        for op in self.get_components_by_kind("outputport"):
            if type(op) is OutputPort:
                output_ports.append(op)
        return output_ports

    def get_workloads(self) -> List[Workload]:
        """
        Retrieve a list of workloads associated with the data product.

        This method filters the components of kind 'workload' that are associated with
        the data product and returns a list containing these workloads.

        Returns:
            List[Workload]: A list of Workload objects that represent the workloads
            associated with the data product.

        Example:
            To retrieve all workloads from a data product 'my_data_product':
            >>> workloads = my_data_product.get_workloads()
        """  # noqa: E501
        workloads: List[Workload] = []
        for wl in self.get_components_by_kind("workload"):
            if type(wl) is Workload:
                workloads.append(wl)
        return workloads

    def get_storage_areas(self) -> List[StorageArea]:
        """
        Retrieve a list of storage areas associated with the data product.

        This method filters the components of kind 'storage' that are associated with
        the data product and returns a list containing these storage areas.

        Returns:
            List[StorageArea]: A list of StorageArea objects that represent the storage
            areas associated with the data product.

        Example:
            To retrieve all storage areas from a data product 'my_data_product':
            >>> storage_areas = my_data_product.get_storage_areas()
        """  # noqa: E501
        storage_areas: List[StorageArea] = []
        for st in self.get_components_by_kind("storage"):
            if type(st) is StorageArea:
                storage_areas.append(st)
        return storage_areas

    def get_observability_APIs(self) -> List[Observability]:
        """
        Retrieve a list of observability APIs associated with the data product.

        This method filters the components of kind 'observability' that are associated with
        the data product and returns a list containing these observability APIs.

        Returns:
            List[Observability]: A list of Observability objects that represent the observability
            APIs associated with the data product.

        Example:
            To retrieve all observability APIs from a data product 'my_data_product':
            >>> observability_apis = my_data_product.get_observability_APIs()
        """  # noqa: E501
        observability_APIs: List[Observability] = []
        for obs in self.get_components_by_kind("observability"):
            if type(obs) is Observability:
                observability_APIs.append(obs)
        return observability_APIs