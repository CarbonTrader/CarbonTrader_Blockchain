import math
from blockchain.Transaction import Transaction
import pytest
from blockchain.Wallet import Wallet


@pytest.fixture
def wallet_one():
    wallet = Wallet()
    wallet.address = "2fa1a6ee-d6e5-45f2-a52a-920b6a3d9274"
    wallet.private_key = "-----BEGIN PRIVATE KEY-----\nMIGEAgEAMBAGByqGSM49AgEGBSuBBAAKBG0wawIBAQQgRAo55xP8plxV5AGw9y9M\nsR1Q3QKfPHZ1xhaXThV+7suhRANCAAS25SL1dLIS4POUeJ50guEvq9PHG96UblH3\nwLzKN/kfwewEptOLGm7RSVkw5ef1pKVOeaJXooLM24/GzvzPv1xg\n-----END PRIVATE KEY-----\n"
    wallet.public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEtuUi9XSyEuDzlHiedILhL6vTxxvelG5R\n98C8yjf5H8HsBKbTixpu0UlZMOXn9aSlTnmiV6KCzNuPxs78z79cYA==\n-----END PUBLIC KEY-----\n"
    return wallet


@pytest.fixture
def wallet_two():
    wallet = Wallet()
    wallet.address = "209038bf-7e96-407c-a23b-a5ea1eb9bf93"
    wallet.private_key = "-----BEGIN PRIVATE KEY-----\nMIGEAgEAMBAGByqGSM49AgEGBSuBBAAKBG0wawIBAQQgBzyHDVT0AN7GVk9FFg+r\nsr5UtyhBOL6MshMaSmK3pEyhRANCAARpNg497txvMQ4eJxE3a2jAs5aECUtQTk5s\nByv5MSk2M2jAmHBvWaMtHboKEWfZlV14z51E8qwCJ9I9l380JE0w\n-----END PRIVATE KEY-----\n"
    wallet.public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEaTYOPe7cbzEOHicRN2towLOWhAlLUE5O\nbAcr+TEpNjNowJhwb1mjLR26ChFn2ZVdeM+dRPKsAifSPZd/NCRNMA==\n-----END PUBLIC KEY-----\n"
    return wallet


def testsquare(wallet_one, wallet_two):
    assert wallet_two.private_key == "-----BEGIN PRIVATE KEY-----\nMIGEAgEAMBAGByqGSM49AgEGBSuBBAAKBG0wawIBAQQgBzyHDVT0AN7GVk9FFg+r\nsr5UtyhBOL6MshMaSmK3pEyhRANCAARpNg497txvMQ4eJxE3a2jAs5aECUtQTk5s\nByv5MSk2M2jAmHBvWaMtHboKEWfZlV14z51E8qwCJ9I9l380JE0w\n-----END PRIVATE KEY-----\n"

def test_transaction_creation(wallet_one,wallet_two):
    trans = Transaction("id1","retire","serial",wallet_one,wallet_two.address)
    expected_transaction = {'id': 'id1', 
                            'timestamp': trans.timestamp, 
                            'recipient_address': '209038bf-7e96-407c-a23b-a5ea1eb9bf93', 
                            'sender_address': '2fa1a6ee-d6e5-45f2-a52a-920b6a3d9274', 
                            'carbon_trader_serial': 'serial', 
                            'transaction_type': 'retire', 
                            'public_key': wallet_one.public_key, 
                            'hash': 'e6da2e34da993cfa0d5145b35680a84c6c8a1c683e820d8bd1fbcd70006e2c20', 
                            'signature': (544406344018823059341540447880134850642516758197857779300873919306746727276, 64941673004990282562783024019716155240991015636218944715298466460379666033916)}
    assert expected_transaction == trans .__dict__   
