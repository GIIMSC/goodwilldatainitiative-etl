import json
import logging

import requests


def get_member_id(members_api_url: str, client_id: str, site_name: str, **kwargs):
    """
    This simple function requests information about local Goodwills, as described in a
    secure API managed by GII. The API uses a basic auth flow, in which we use a client key to 
    request an access token, and we exchange the access token for information about the local Goodwills.
    """
    token_url = f"{members_api_url}/Api/AuthToken/GetAuthToken?clientKey={client_id}"
    response_with_token = requests.get(token_url)
    token = response_with_token.json()["Token"]
    orgs_url = f"{members_api_url}/API/CRMAPI/GetActiveOrgs?authToken={{{token}}}"
    response_with_orgs = requests.get(orgs_url)

    try:
        all_orgs = response_with_orgs.json()
    except json.decoder.JSONDecodeError:
        logging.error("The response from `GetActiveOrgs` did not return JSON.")
        raise

    try:
        member_id = next(
            goodwill["id"] for goodwill in all_orgs if goodwill["name"] == site_name
        )
    except StopIteration:
        logging.error(
            "The name of the Goodwill in `siteinfo.py` cannot be found in the GII Web API."
        )
        raise

    return member_id
