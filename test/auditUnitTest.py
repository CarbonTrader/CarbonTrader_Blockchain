from typing import List

import pytest

from controllers.AuditController import AuditController


@pytest.fixture
def blockchain():
    return [
        {
            "timestamp": 1663608898836871600,
            "last_hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "hash": "a571e724db5acd737c9e45f798dae743aed2d55484567f1d307521673c5b39ee",
            "merkle_root": None,
            "number_transactions": 0,
            "transactions_hashes": []
        },
        {
            "timestamp": 1665987251019110142,
            "last_hash": "a571e724db5acd737c9e45f798dae743aed2d55484567f1d307521673c5b39ee",
            "hash": "1f23d3f698424d4e491cce9ed4f07553f2476b8b5904123214c54be0edaa9bb9",
            "merkle_root": "fb63851a5a598b86956b3c3fdafc361a8c744f28dd294fa33e79ee99d6ad15eb",
            "number_transactions": 3,
            "transactions_hashes": [
                "a489044d36c5faf1b4bfb8ee43ad4ad3bc94852e75bf5a34260dbf753a759f6e",
                "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5",
                "bbdbbc5cafc19451daeac4705b710f79ea44e531496652776b9fcc0f26abfcf3"
            ]
        },
        {
            "timestamp": 1665987425619192365,
            "last_hash": "1f23d3f698424d4e491cce9ed4f07553f2476b8b5904123214c54be0edaa9bb9",
            "hash": "4506b659ec6d58794ff5feb1a0c4276ba77228abd300b9c5c73bb0851edd4b13",
            "merkle_root": "b82d40638c42b0f394692ccc23b99658039f16b7fee1d3468ac40995638ce5b6",
            "number_transactions": 3,
            "transactions_hashes": [
                "dfcb15c31e3cbdaf781f142b2f362c8e98eda1085d9e4786fbad8d5cab66a047",
                "42a3c90896001343d30fa3acd090fd3b7b9cf9311b019db29a0dad579b3cde67",
                "59324e3a4400c4e9abf3679b7d0a33e0db742238e914c82485d5f6fb22f8141e"
            ]
        }
    ]


@pytest.fixture
def tamper_blockchain():
    return [
        {
            "timestamp": 1663608898836871600,
            "last_hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "hash": "a571e724db5acd737c9e45f798dae743afd2d55484567f1d307521673c5b39ee",
            "merkle_root": None,
            "number_transactions": 0,
            "transactions_hashes": []
        },
        {
            "timestamp": 1665987251019110142,
            "last_hash": "a571e724ob5acd737c9e45f798dae743aed2d55484567f1d307521673c5b39ee",
            "hash": "1f23d3f698424d4e491cce9ed4f07553f2476b8b5904123214c54be0edaa9bb9",
            "merkle_root": "fb63851f5a598b86956b3c3fdafc361a8c744f28dd294fa33e79ee99d6ad15eb",
            "number_transactions": 3,
            "transactions_hashes": [
                "a489044d36c5faf1b4bfb8ee43ad4ad3bc94852e75bf5a34260dbf753a759f6e",
                "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5",
                "bbdbbc5cafc19451daeac4705b710f79ea44e531496652776b9fcc0f26abfcf3"
            ]
        },
        {
            "timestamp": 1665987425619192365,
            "last_hash": "1f23d3gg98424d4e49fcce9ed4f07553f2476b8b5904123214c54be0edaa9bb9",
            "hash": "4506b659ec6d58794ff5feb1a0c4276ba77228abd300b9c5c73bb0851edd4b13",
            "merkle_root": "b82d40638c52b0f394692ccc23b99658039f16b7fee1d3468ac40995638ce5b6",
            "number_transactions": 3,
            "transactions_hashes": [
                "dfcb15c31e3cbdaf781f142b2f362c8e98eda1085d9e4786fbad8d5cab66a047",
                "42a3c90896001343d30fa3acd090fd3b7b9cf9311b019db29a0dad579b3cde67",
                "59324e3a4400c4e9abf3679b7d0a33e0db742238e914c82485d5f6fb22f8141e"
            ]
        }
    ]


def test_valid_chain_deep_block_hash(blockchain):
    assert AuditController.deep_block_hash_test(blockchain)


def test_invalid_chain_deep_block_hash(tamper_blockchain):
    assert AuditController.deep_block_hash_test(tamper_blockchain) == False


def test_valid_chain_light_block_hash(blockchain):
    assert AuditController.light_block_hash_test(blockchain)


def test_invalid_chain_light_block_hash(tamper_blockchain):
    assert AuditController.light_block_hash_test(tamper_blockchain) == False


def test_validate_transaction_in_block(blockchain):
    assert AuditController.validate_transaction_in_block(blockchain, [[1,
                                                                       "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5"]]) == [
               True]


def test_validate_transaction_different_block(blockchain):
    assert AuditController.validate_transaction_in_block(blockchain, [[2,
                                                                       "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5"]]) == [
               False]


def test_validate_transaction_multiple_blocks(blockchain):
    assert AuditController.validate_transaction_in_block(blockchain, [[1,
                                                                       "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5"],
                                                                      [2,
                                                                       "bf4eaa637f6d0b2a734c5ac15970450e0331eef3c97f772d8f8d0705c58fa1b5"]]) == [
               True, False]
