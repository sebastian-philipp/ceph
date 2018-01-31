# -*- coding: utf-8 -*-
"""
 *  Copyright (C) 2011-2016, it-novum GmbH <community@openattic.org>
 *
 *  openATTIC is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; version 2.
 *
 *  This package is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
"""
from unittest import TestCase

from ..models.taskqueue import Task, task, chain


@task(description='Adding {0} and {1}')
def add(x, y):
    return x + y


def wait_percent(current, total=None):
    total = total if total else current
    return float(total - current) / float(total) * 100


@task(percent=wait_percent)
def wait(current_times, total_times=None):
    if current_times > 1:
        return wait(current_times - 1, total_times if total_times else current_times)
    else:
        return True


@task
def wait_add(times, x, y):
    return chain([wait(times), add(x, y)])


def run_task_till_result(this_task):
    assert isinstance(this_task, Task)
    while True:
        res = this_task.run_once()
        if not isinstance(res, Task):
            return res
        else:
            this_task = res


class TaskTestCase(TestCase):

    def test_serialize(self):
        tasks = \
            [
                Task('m.f', [1, 2], {"param": 3}),
                add(3, 4),
                wait(10),
                chain([add(3, 4), add(6, 7)]),
                chain([chain([add(3, 4), add(6, 7)]), chain([add(3, 4), add(6, 7)])]),
            ]
        for t in tasks:
            self.assertEqual(Task.deserialize(t.serialize()).serialize(), t.serialize())

    def test_wrapper(self):
        t = add(3, 4)
        self.assertEqual(t.func_reference, 'dashboard_v2.tests.test_taskqueue.add',
                         'Check your Python environment. It used to work with "-t . -v 2"')
        self.assertEqual(t.args, [3, 4])
        self.assertEqual(t.kwargs, {})

    def test_evaluate(self):
        jobs = \
            [
                (add(3, 4), 7),
                (chain([add(10, 12), add(3, 4)]), 7),
                (chain([wait(10), add(3, 4)]), 7),
                (wait(10), True),
                (wait_add(4, 1, 2), 3)
            ]
        for t, res in jobs:
            self.assertEqual(run_task_till_result(t), res)
            t = Task.deserialize(t.serialize())
            self.assertEqual(run_task_till_result(t), res)

    def test_percent(self):
        chains = \
            [
                chain([add(1, 2), add(1, 2), add(1, 2), add(1, 2)]),
                chain([chain([add(1, 2), add(1, 2), add(1, 2), add(1, 2)])]),
                wait(4)
            ]
        for my_chain in chains:
            self.assertEquals(my_chain.percent(), 0)
            my_chain = my_chain.run_once()
            self.assertEquals(my_chain.percent(), 25)
            my_chain = my_chain.run_once()
            self.assertEquals(my_chain.percent(), 50)
            my_chain = my_chain.run_once()
            self.assertEquals(my_chain.percent(), 75)
            result = my_chain.run_once()
            self.assertIn(result, [3, True])

    def test_description(self):
        tq = add.delay(1,2)
        self.assertEqual(tq.description, 'Adding 1 and 2')
