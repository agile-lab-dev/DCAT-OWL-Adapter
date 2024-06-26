from __future__ import annotations

from requests.auth import HTTPBasicAuth
import requests
import uuid
import json

from datetime import date
from typing import Optional

import sys
import os

import yaml
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

import asyncio 

from src.dependencies import unpack_provisioning_request
from src.utility.parsing_pydantic_models import parse_yaml_with_model

from starlette.responses import Response

from src.app_config import app
from src.check_return_type import check_response
from src.models.api_models import (
    ProvisioningRequest,
    ProvisioningStatus,
    SystemErr,
    UpdateAclRequest,
    ValidationError,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
)

from src.models.data_product_descriptor import *
from src.DCAT import *

from src.utility.logger import get_logger

logger = get_logger()



# Example usage
catalog = DCATCatalog()

@app.get(
    "/v1/resources",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
async def resources(filter: str, offset = None, limit = None) -> Response:
    print(filter)
    results = catalog.list_fibo_business_terms(None, filter)

    return results, 200, {'Content-Type': 'application/json'}




@app.post(
    "/v1/provision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
async def provision(
    body: ProvisioningRequest,
) -> Response:
    """
    Deploy a data product or a single component starting from a provisioning descriptor
    """

    dp = await unpack_provisioning_request(body)
    print("------------------DESCRIPTOR PARSED-----------------------------")
    print(dp)

    for component in dp[0].components:
        if component.kind == "outputport":
            cdict: dict = component.dict()
            rename_key(cdict['dataContract'], 'schema_', 'schema')
            outputport = OutputPort(**cdict)
            columns = outputport.dataContract.schema_

            catalog.add_dataset(
                outputport.name,
                outputport.fullyQualifiedName,
                outputport.description,
                theme="http://example.org/themes/"+dp[0].domain,
                keywords=[dp[0].name],
                issued=date.today().strftime("%Y-%m-%d"),
                fields=  [
                    {
                        'name': column.name,
                        'type': column.dataType,
                        'description': column.description,
                        'business_terms': [ f'{tag.tagFQN}'  for tag in (column.tags if column.tags is not None else []) ]
                    } for column in columns
                ]
            )

            




    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)




def rename_key(dictionary, old_key, new_key):
    if old_key in dictionary:
        dictionary[new_key] = dictionary.pop(old_key)
    else:
        raise KeyError(f"Key '{old_key}' not found in dictionary")


@app.get("/catalog")
def get_catalog() -> Response:
    
    return catalog.serialize_catalog(), 200, {'Content-Type': 'application/ld+json'}



@app.get(
    "/v1/provision/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_status(token: str) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/unprovision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def unprovision(
    body: ProvisioningRequest,
) -> Response:
    """
    Undeploy a data product or a single component
    given the provisioning descriptor relative to the latest complete provisioning request
    """  # noqa: E501

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/updateacl",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def updateacl(
    body: UpdateAclRequest,
) -> Response:
    """
    Request the access to a specific provisioner component
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/validate",
    response_model=None,
    responses={"200": {"model": ValidationResult}, "500": {"model": SystemErr}},
    tags=["SpecificProvisioner"],
)
def validate(body: ProvisioningRequest) -> Response:
    """
    Validate a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v2/validate",
    response_model=None,
    responses={
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def async_validate(
    body: ValidationRequest,
) -> Response:
    """
    Validate a deployment request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.get(
    "/v2/validate/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ValidationStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_validation_status(
    token: str,
) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)
