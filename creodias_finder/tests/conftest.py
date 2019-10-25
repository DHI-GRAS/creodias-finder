import os
import uuid
from pathlib import Path
import pytest

SMALL_FILES = ['e7775c05-6cd2-5e2e-9439-593415de220c',
               '87b3dba1-5eca-5b23-9dac-b7cb2d7a25c8',
               '8d560dc5-d127-559f-b4cf-4e26e5d2920b',
               'd0e0e0b5-5008-5096-8726-63c242c63511']


@pytest.fixture
def password():
    """Returns password if it exists as env variable otherwise raise error"""
    passw = os.environ['CREODIAS_PASSWORD']
    if passw:
        return passw
    raise ValueError("Set environment variable CREODIAS_PASSWORD")


@pytest.fixture
def username():
    """Returns username if it exists as env variable otherwise raise error"""
    user = os.environ['CREODIAS_USERNAME']
    if user:
        return user
    raise ValueError("Set environment variable CREODIAS_USERNAME")


@pytest.fixture
def uid():
    """Returns random UID"""
    from random import choice
    return choice(SMALL_FILES)


@pytest.fixture
def uids():
    return SMALL_FILES


@pytest.fixture(params=[None, str(uuid.uuid1()), Path(str(uuid.uuid1()))])
def workdir(request):
    """Parameterized fixture, to test various input types"""
    yield request.param
