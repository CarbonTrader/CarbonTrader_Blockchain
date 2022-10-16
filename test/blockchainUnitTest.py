from typing import List

import pytest

from blockchain.Block import Block
from blockchain.Blockchain import Blockchain
from blockchain.CryptoHash import CryptoHash
from blockchain.Merkle import Merkle
from blockchain.Transaction import Transaction
from blockchain.Wallet import Wallet


@pytest.fixture
def wallet_one() -> Wallet:
    wallet = Wallet()
    wallet.address = "2fa1a6ee-d6e5-45f2-a52a-920b6a3d9274"
    wallet.private_key = "-----BEGIN PRIVATE KEY-----\nMIGEAgEAMBAGByqGSM49AgEGBSuBBAAKBG0wawIBAQQgRAo55xP8plxV5AGw9y9M\nsR1Q3QKfPHZ1xhaXThV+7suhRANCAAS25SL1dLIS4POUeJ50guEvq9PHG96UblH3\nwLzKN/kfwewEptOLGm7RSVkw5ef1pKVOeaJXooLM24/GzvzPv1xg\n-----END PRIVATE KEY-----\n"
    wallet.public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEtuUi9XSyEuDzlHiedILhL6vTxxvelG5R\n98C8yjf5H8HsBKbTixpu0UlZMOXn9aSlTnmiV6KCzNuPxs78z79cYA==\n-----END PUBLIC KEY-----\n"
    return wallet


@pytest.fixture
def wallet_two() -> Wallet:
    wallet = Wallet()
    wallet.address = "209038bf-7e96-407c-a23b-a5ea1eb9bf93"
    wallet.private_key = "-----BEGIN PRIVATE KEY-----\nMIGEAgEAMBAGByqGSM49AgEGBSuBBAAKBG0wawIBAQQgBzyHDVT0AN7GVk9FFg+r\nsr5UtyhBOL6MshMaSmK3pEyhRANCAARpNg497txvMQ4eJxE3a2jAs5aECUtQTk5s\nByv5MSk2M2jAmHBvWaMtHboKEWfZlV14z51E8qwCJ9I9l380JE0w\n-----END PRIVATE KEY-----\n"
    wallet.public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEaTYOPe7cbzEOHicRN2towLOWhAlLUE5O\nbAcr+TEpNjNowJhwb1mjLR26ChFn2ZVdeM+dRPKsAifSPZd/NCRNMA==\n-----END PUBLIC KEY-----\n"
    return wallet


@pytest.fixture
def mock_transactions(wallet_one, wallet_two) -> List[Transaction]:
    return [Transaction("id", "type", "serial", wallet_one, "recipient"),
            Transaction("id2", "type", "serial", wallet_two, "recipient"),
            Transaction("id3", "type", "serial", wallet_one, "recipient"),
            Transaction("id4", "type", "serial", wallet_two, "recipient"),
            Transaction("id5", "type", "serial", wallet_two, "recipient")]


@pytest.fixture(scope="module")
def mock_block_chain() -> Blockchain:
    return Blockchain()


@pytest.fixture(scope="module")
def bitcoin_info() -> dict:
    import requests
    URL = "https://blockchain.info/rawblock/000000000000000000079b7da5cea599464404dbe339759919e00b48f15f8290?cors=true"
    r = requests.get(URL).json()
    official_merkle_root = r.get("mrkl_root")
    trans = r.get("tx")
    trans = [txt.get("hash") for txt in trans]
    return {"official_merkle_root": official_merkle_root, "trans": trans}


def test_transaction_creation(wallet_one: Wallet, wallet_two: Wallet) -> None:
    trans: Transaction = Transaction("id1", "retire", "serial", wallet_one, wallet_two.address)
    expected_transaction = {'id': "id1",
                            'timestamp': trans.timestamp,
                            'recipient_address': wallet_two.address,
                            'sender_address': wallet_one.address,
                            'carbon_trader_serial': "serial",
                            'transaction_type': "retire",
                            'public_key': wallet_one.public_key,
                            'hash': CryptoHash.get_hash("id1", trans.timestamp, wallet_two.address, wallet_one.address,
                                                        "serial", "retire", wallet_one.public_key),
                            'signature': trans.signature}
    assert expected_transaction == trans.__dict__


def test_wallet_signature_simple_data(wallet_one: Wallet):
    data = {'foo': 'bar'}
    signature = wallet_one.sign(data)
    wallet_one.deserialize_public_key()
    assert Wallet.verify(wallet_one.public_key, data, signature)


def test_wallet_signature_complex_data(wallet_one: Wallet, wallet_two: Wallet):
    data = {"foo": "bar", "array": [1, 2, 3], "object": {"array_two": [2, 2, 5]}}
    signature = wallet_one.sign(data)
    wallet_one.deserialize_public_key()
    assert Wallet.verify(wallet_one.public_key, data, signature)


def test_transaction_signature(wallet_one, wallet_two, mock_transactions):
    trans: Transaction = Transaction("id1", "retire", "serial", wallet_one, wallet_two.address)
    assert Block.is_valid_signature(wallet_one.public_key, trans.__dict__, trans.signature)


def test_merkle_root_against_bitcoin(bitcoin_info):
    recreated_merkle_root = Merkle.merkle_root(bitcoin_info["trans"])
    assert recreated_merkle_root == bitcoin_info["official_merkle_root"]


def test_merkle_proof_against_bitcoin(bitcoin_info):
    import random
    tx = random.choice(bitcoin_info["trans"])
    merkle_proof_ = Merkle.merkle_proof(bitcoin_info["trans"], tx)
    assert Merkle.merkle_proof_root(merkle_proof_, tx) == bitcoin_info["official_merkle_root"]


def test_block_creation(mock_transactions, mock_block_chain):
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    expected_block = Block(mock_block_chain.chain[0].hash)
    expected_block.timestamp = new_block.timestamp
    expected_block.number_transactions = len(transaction_hashes)
    expected_block.transactions_hashes = transaction_hashes[:]
    expected_block.merkle_root = Merkle.merkle_root(transaction_hashes[:])
    expected_block.hash = CryptoHash.get_hash(new_block.timestamp, new_block.last_hash, new_block.merkle_root,
                                              new_block.number_transactions, new_block.transactions_hashes)
    assert expected_block.hash == new_block.hash


def test_block_validation(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    assert Block.is_valid_block(new_block.__dict__, mock_block_chain.chain[-1], transactions, transaction_hashes)


def test_block_validation_wrong_transactions(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    assert not Block.is_valid_block(new_block.__dict__, mock_block_chain.chain[-1], transactions,
                                    transaction_hashes[:-1])


def test_block_validation_wrong_merkle_root(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    new_block.merkle_root = new_block.merkle_root[:-1]
    assert not Block.is_valid_block(new_block.__dict__, mock_block_chain.chain[-1], transactions, transaction_hashes)


def test_block_validation_last_hash_not_valid(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    new_block.last_hash = new_block.hash
    assert not Block.is_valid_block(new_block.__dict__, mock_block_chain.chain[-1], transactions, transaction_hashes)


def test_block_validation_hash_not_valid(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    new_block.hash = new_block.last_hash
    assert not Block.is_valid_block(new_block.__dict__, mock_block_chain.chain[-1], transactions, transaction_hashes)


def test_add_new_block(mock_block_chain, mock_transactions):
    transactions = [trans.__dict__ for trans in mock_transactions]
    transaction_hashes = [trans.hash for trans in mock_transactions]
    new_block = mock_block_chain.create_not_verify_block(transaction_hashes)
    mock_block_chain.add_block(new_block)

    assert mock_block_chain.chain[0].hash == mock_block_chain.chain[1].last_hash
