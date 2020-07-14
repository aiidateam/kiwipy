# -*- coding: utf-8 -*-
import functools

import pytest

from . import support

NUM_TASKS = 100
DEFAULT_TASK_SIZE = 1024


def test_kiwi_send(bench_queue, benchmark):
    """Benchmark just getting messages"""
    num_tasks = NUM_TASKS
    task_size = DEFAULT_TASK_SIZE

    benchmark(support.send_tasks, num_tasks, task_size, bench_queue)


def test_pika_send(pika_channel, pika_queue, benchmark):
    """Benchmark just getting messages"""
    num_tasks = NUM_TASKS
    task_size = DEFAULT_TASK_SIZE

    benchmark(support.pika_send_tasks, num_tasks, task_size, pika_channel, pika_queue)


def test_kiwi_get(bench_queue, benchmark):
    """Benchmark just getting messages"""
    num_tasks = NUM_TASKS
    task_size = DEFAULT_TASK_SIZE

    send = functools.partial(support.send_tasks, num_tasks, task_size, bench_queue)
    benchmark.pedantic(support.get_tasks, args=(num_tasks, bench_queue), setup=send, iterations=1, rounds=15)


def test_pika_get(pika_channel, pika_queue, benchmark):
    """Benchmark just getting messages"""
    num_tasks = NUM_TASKS
    task_size = DEFAULT_TASK_SIZE

    send = functools.partial(support.pika_send_tasks, num_tasks, task_size, pika_channel, pika_queue)
    benchmark.pedantic(
        support.pika_get_tasks, args=(num_tasks, pika_channel, pika_queue), setup=send, iterations=1, rounds=10
    )


@pytest.mark.parametrize('task_size', [32, 128, 512, 1024, 2048])
def test_kiwi_send_get(bench_queue, benchmark, task_size):
    """Benchmark sending and getting messages"""
    num_tasks = NUM_TASKS

    def do_test():
        support.send_tasks(num_tasks, task_size, bench_queue)
        support.get_tasks(num_tasks, bench_queue)

    benchmark(do_test)


@pytest.mark.parametrize('task_size', [32, 128, 512, 1024, 2048])
def test_pika_send_get(pika_channel, pika_queue, benchmark, task_size):
    """Benchmark sending and getting messages"""
    num_tasks = NUM_TASKS

    def do_test():
        support.pika_send_tasks(num_tasks, task_size, pika_channel, pika_queue)
        support.pika_get_tasks(num_tasks, pika_channel, pika_queue)

    benchmark(do_test)
