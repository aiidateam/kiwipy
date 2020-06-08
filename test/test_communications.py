# -*- coding: utf-8 -*-
"""Tests for the :mod:`kiwipy.communications` module."""
from __future__ import absolute_import
import pytest

from kiwipy import CommunicatorHelper


class Subscriber:
    """Test class that mocks a subscriber."""


class Communicator(CommunicatorHelper):

    def task_send(self, task, no_reply=False):
        pass

    def rpc_send(self, recipient_id, msg):
        pass

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        pass


@pytest.fixture
def communicator():
    """Return an instance of `Communicator`."""
    return Communicator()


@pytest.fixture
def subscriber():
    """Return an instance of `Subscriber`."""
    return Subscriber()


def test_add_rpc_subscriber(communicator, subscriber):
    """Test the `Communicator.add_rpc_subscriber` method."""
    assert communicator.add_rpc_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert communicator.add_rpc_subscriber(subscriber, identifier) == identifier


def test_remove_rpc_subscriber(communicator, subscriber):
    """Test the `Communicator.remove_rpc_subscriber` method."""
    identifier = communicator.add_rpc_subscriber(subscriber)
    communicator.remove_rpc_subscriber(identifier)


def test_add_broadcast_subscriber(communicator, subscriber):
    """Test the `Communicator.add_broadcast_subscriber` method."""
    assert communicator.add_broadcast_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert communicator.add_broadcast_subscriber(subscriber, identifier) == identifier


def test_remove_broadcast_subscriber(communicator, subscriber):
    """Test the `Communicator.remove_broadcast_subscriber` method."""
    identifier = communicator.add_broadcast_subscriber(subscriber)
    communicator.remove_broadcast_subscriber(identifier)


def test_add_task_subscriber(communicator, subscriber):
    """Test the `Communicator.add_task_subscriber` method."""
    assert communicator.add_task_subscriber(subscriber) is None


def test_remove_task_subscriber(communicator, subscriber):
    """Test the `Communicator.remove_task_subscriber` method."""
    communicator.add_task_subscriber(subscriber)
    communicator.remove_task_subscriber(subscriber)
