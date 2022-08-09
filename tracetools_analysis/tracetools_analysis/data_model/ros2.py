# Copyright 2019 Robert Bosch GmbH
# Copyright 2020-2021 Christophe Bedard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for ROS 2 data model."""

from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd

from . import DataModel
from . import DataModelIntermediateStorage
from ..processor import EventMetadata


Obj = Dict[str, Any]

PID = int
LocalHandle = int

GlobalHandle = Tuple[PID, LocalHandle]
"""
Unique identification of a ROS 2 object consisting of a PID and a memory pointer.

TODO: Also add host to support multi-hosts analysis (pid + memory pointer is unique only within one host).
"""

MessageHandle = GlobalHandle
"""
MessageHandle can be repeated for many messages within same process.
It can NOT be used to match publications and subscriptions.
"""

CallbackHandle = GlobalHandle

Timestamp = int
TopicName = str
# TODO: switch to gid once rmw_cyclonedds correctly supports it
# MessageUID = Tuple[int, Tuple[int, ...]]
MessageUID = Tuple[Timestamp, TopicName]
"""
MessageUID uniquely identifies a specific message in that was published in the system.
It is
It can be used to match publications and subscriptions (one to many).
"""


class Ros2DataModel(DataModel):
    """
    Container to model pre-processed ROS 2 data for analysis.

    This aims to represent the data in a ROS 2-aware way.
    """

    def __init__(self) -> None:
        """Create a Ros2DataModel."""
        super().__init__()

        # Objects (one-time events, usually when something is created)

        # DDS writers are created for publishers and services (and actions) by RMW
        self._dds_writers: DataModelIntermediateStorage = []
        # DDS readers are created for subscriptions and services (and actions) by RMW
        self._dds_readers: DataModelIntermediateStorage = []

        self._rcl_contexts: DataModelIntermediateStorage = []

        # TODO: How to correctly associate rcl_context with rcl_node?
        #       Is it possible at all (given current tracepoints)?

        self._rcl_nodes: DataModelIntermediateStorage = []

        # When a publisher is created, the sequence of events is:
        # 1. dds:create_writer -> 2. ros2:rmw_publisher_init -> 3. ros2:rcl_publisher_init
        self._rmw_publishers: DataModelIntermediateStorage = []
        self._rcl_publishers: DataModelIntermediateStorage = []

        # When a subscription is created, the sequence of events is:
        # 1. dds:create_reader -> 2. ros2:rmw_subscription_init -> 3. ros2:rcl_subscription_init
        # 4. -> ros2:rclcpp_subscription_init
        self._rmw_subscriptions: DataModelIntermediateStorage = []
        self._rcl_subscriptions: DataModelIntermediateStorage = []
        self._rclcpp_subscriptions: DataModelIntermediateStorage = []

        self._rcl_services: DataModelIntermediateStorage = []
        self._rcl_clients: DataModelIntermediateStorage = []
        self._rcl_timers: DataModelIntermediateStorage = []
        self._timer_node_links: DataModelIntermediateStorage = []
        self._callback_objects: DataModelIntermediateStorage = []
        self._callback_symbols: DataModelIntermediateStorage = []
        self._lifecycle_state_machines: DataModelIntermediateStorage = []

        # Events (multiple instances, may not have a meaningful index)

        self._dds_write_instances: DataModelIntermediateStorage = []

        self._dds_read_instances: DataModelIntermediateStorage = []
        # publish flow: rclcpp/rclpy publish -> rcl publish -> rmw publish -> dds write
        self._rclcpp_publish_instances: DataModelIntermediateStorage = []
        self._rcl_publish_instances: DataModelIntermediateStorage = []
        self._rmw_publish_instances: DataModelIntermediateStorage = []

        self._rmw_take_instances: DataModelIntermediateStorage = []
        self._rcl_take_instances: DataModelIntermediateStorage = []
        self._rclcpp_take_instances: DataModelIntermediateStorage = []

        self._callback_instances: DataModelIntermediateStorage = []

        self._rcl_lifecycle_transitions: DataModelIntermediateStorage = []

    @staticmethod
    def get_unique_index(data: DataModelIntermediateStorage, keys: List[str]) -> pd.MultiIndex:
        return pd.MultiIndex.from_tuples(
            tuples=[tuple(d[key] for key in keys) for d in data],
            names=keys,
        )

    @staticmethod
    def get_std_unique_index(data: DataModelIntermediateStorage, key: str) -> pd.MultiIndex:
        # TODO: add also host to support multi-hosts analysis (pid + memory pointer is unique within one host)
        return Ros2DataModel.get_unique_index(data=data, keys=['pid', key])

    @staticmethod
    def _to_dataframe(data: DataModelIntermediateStorage, key: str, columns: List[str]) -> pd.DataFrame:
        return pd.DataFrame(
            data=data,
            index=Ros2DataModel.get_std_unique_index(data=data, key=key),
            columns=['timestamp', 'tid', 'cpu_id'] + columns,
        )

    def add_dds_writer(
        self, metadata: EventMetadata,
        dds_writer_handle: int, dds_topic_name: str,
        gid_prefix: Tuple[int, ...], gid_entity: Tuple[int, ...], gid: Tuple[int, ...],
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'dds_writer_handle': dds_writer_handle,
            'dds_topic_name': dds_topic_name,
            'gid_prefix': gid_prefix,
            'gid_entity': gid_entity,
            'gid': gid,
        }
        self._dds_writers.append(obj)
        return obj

    def _finalize_dds_writers(self):
        self.dds_writers = self._to_dataframe(
            data=self._dds_writers,
            key='dds_writer_handle',
            columns=['dds_topic_name', 'gid_prefix', 'gid_entity', 'gid'],
        )

    def add_dds_reader(
        self, metadata: EventMetadata,
        dds_reader_handle: int, dds_topic_name: str,
        gid_prefix: Tuple[int, ...], gid_entity: Tuple[int, ...], gid: Tuple[int, ...],
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'dds_reader_handle': dds_reader_handle,
            'dds_topic_name': dds_topic_name,
            'gid_prefix': gid_prefix,
            'gid_entity': gid_entity,
            'gid': gid,
        }
        self._dds_readers.append(obj)
        return obj

    def _finalize_dds_readers(self):
        self.dds_readers = self._to_dataframe(
            data=self._dds_readers,
            key='dds_reader_handle',
            columns=['dds_topic_name', 'gid_prefix', 'gid_entity', 'gid'],
        )

    def add_dds_write_instance(
        self, metadata: EventMetadata,
        dds_writer_handle: int, message_handle: int, message_timestamp: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'dds_writer_handle': dds_writer_handle,
            'message_handle': message_handle,
            'message_timestamp': message_timestamp,
        }
        self._dds_write_instances.append(obj)
        return obj

    def _finalize_dds_write_instances(self):
        self.dds_write_instances = pd.DataFrame(
            data=self._dds_write_instances,
            columns=['timestamp', 'pid', 'tid', 'cpu_id', 'dds_writer_handle', 'message_handle', 'message_timestamp'],
        )

    def add_dds_read_instance(
        self, metadata: EventMetadata,
        dds_reader_handle: int, message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'dds_reader_handle': dds_reader_handle,
            'message_handle': message_handle,
        }
        self._dds_read_instances.append(obj)
        return obj

    def _finalize_dds_read_instances(self):
        self.dds_read_instances = pd.DataFrame(
            data=self._dds_read_instances,
            columns=['timestamp', 'pid', 'tid', 'cpu_id', 'dds_reader_handle', 'message_handle'],
        )

    def add_rcl_context(
        self, metadata: EventMetadata,
        rcl_context_handle: int, tracetools_version: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_context_handle': rcl_context_handle,
            'tracetools_version': tracetools_version,
        }
        self._rcl_contexts.append(obj)
        return obj

    def _finalize_rcl_contexts(self):
        self.rcl_contexts = self._to_dataframe(
            data=self._rcl_contexts,
            key='rcl_context_handle',
            columns=['tracetools_version'],
        )

    def add_rcl_node(
        self, metadata: EventMetadata,
        rcl_node_handle: int, rmw_node_handle: int, name: str, namespace: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_node_handle': rcl_node_handle,
            'rmw_node_handle': rmw_node_handle,
            'name': name,
            'namespace': namespace,
        }
        self._rcl_nodes.append(obj)
        return obj

    def _finalize_rcl_nodes(self):
        self.rcl_nodes = self._to_dataframe(
            data=self._rcl_nodes,
            key='rcl_node_handle',
            columns=['rmw_node_handle', 'name', 'namespace'],
        )

    def add_rmw_publisher(
        self, metadata: EventMetadata,
        rmw_publisher_handle: int, gid: Tuple[int, ...],
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rmw_publisher_handle': rmw_publisher_handle,
            'gid': gid,
        }
        self._rmw_publishers.append(obj)
        return obj

    def _finalize_rmw_publishers(self):
        self.rmw_publishers = self._to_dataframe(
            data=self._rmw_publishers,
            key='rmw_publisher_handle',
            columns=['gid'],
        )

    def add_rcl_publisher(
        self, metadata: EventMetadata,
        rcl_publisher_handle: int, rcl_node_handle: int, rmw_publisher_handle: int,
        topic_name: str, depth: int
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_publisher_handle': rcl_publisher_handle,
            'rcl_node_handle': rcl_node_handle,
            'rmw_publisher_handle': rmw_publisher_handle,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._rcl_publishers.append(obj)
        return obj

    def _finalize_rcl_publishers(self):
        self.rcl_publishers = self._to_dataframe(
            data=self._rcl_publishers,
            key='rcl_publisher_handle',
            columns=['rcl_node_handle', 'rmw_publisher_handle', 'topic_name', 'depth'],
        )

    def add_rclcpp_publish_instance(
        self, metadata: EventMetadata,
        message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'message_handle': message_handle,
        }
        self._rclcpp_publish_instances.append(obj)
        return obj

    def _finalize_rclcpp_publish_instances(self):
        self.rclcpp_publish_instances = pd.DataFrame(
            data=self._rclcpp_publish_instances,
            columns=['timestamp', 'pid', 'tid', 'cpu_id', 'message_handle'],
        )

    def add_rcl_publish_instance(
        self, metadata: EventMetadata,
        rcl_publisher_handle: int, message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_publisher_handle': rcl_publisher_handle,
            'message_handle': message_handle,
        }
        self._rcl_publish_instances.append(obj)
        return obj

    def _finalize_rcl_publish_instances(self):
        self.rcl_publish_instances = pd.DataFrame(
            data=self._rcl_publish_instances,
            columns=['timestamp', 'pid', 'tid', 'cpu_id', 'rcl_publisher_handle', 'message_handle'],
        )

    def add_rmw_publish_instance(
        self, metadata: EventMetadata,
        message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'message_handle': message_handle,
        }
        self._rmw_publish_instances.append(obj)
        return obj

    def _finalize_rmw_publish_instances(self):
        self.rmw_publish_instances = pd.DataFrame(
            data=self._rmw_publish_instances,
            columns=['timestamp', 'pid', 'tid', 'cpu_id', 'message_handle'],
        )

    def add_rmw_subscription(
        self, metadata: EventMetadata,
        rmw_subscription_handle: int, gid: Tuple[int, ...],
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rmw_subscription_handle': rmw_subscription_handle,
            'gid': gid,
        }
        self._rmw_subscriptions.append(obj)
        return obj

    def _finalize_rmw_subscriptions(self):
        self.rmw_subscriptions = self._to_dataframe(
            data=self._rmw_subscriptions,
            key='rmw_subscription_handle',
            columns=['gid'],
        )

    def add_rcl_subscription(
        self, metadata: EventMetadata,
        rcl_subscription_handle: int, rcl_node_handle: int, rmw_subscription_handle: int,
        topic_name: str, depth: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_subscription_handle': rcl_subscription_handle,
            'rcl_node_handle': rcl_node_handle,
            'rmw_subscription_handle': rmw_subscription_handle,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._rcl_subscriptions.append(obj)
        return obj

    def _finalize_rcl_subscriptions(self):
        self.rcl_subscriptions = self._to_dataframe(
            data=self._rcl_subscriptions,
            key='rcl_subscription_handle',
            columns=['rcl_node_handle', 'rmw_subscription_handle', 'topic_name', 'depth'],
        )

    def add_rclcpp_subscription(
        self, metadata: EventMetadata,
        rclcpp_subscription_handle: int, rcl_subscription_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rclcpp_subscription_handle': rclcpp_subscription_handle,
            'rcl_subscription_handle': rcl_subscription_handle,
        }
        self._rclcpp_subscriptions.append(obj)
        return obj

    def _finalize_rclcpp_subscriptions(self):
        self.rclcpp_subscriptions = self._to_dataframe(
            data=self._rclcpp_subscriptions,
            key='rclcpp_subscription_handle',
            columns=['rcl_subscription_handle'],
        )

    def add_rcl_service(
        self, metadata: EventMetadata,
        rcl_service_handle: int, rcl_node_handle: int, rmw_service_handle: int, rcl_service_name: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_service_handle': rcl_service_handle,
            'rcl_node_handle': rcl_node_handle,
            'rmw_service_handle': rmw_service_handle,
            'rcl_service_name': rcl_service_name,
        }
        self._rcl_services.append(obj)
        return obj

    def _finalize_rcl_services(self):
        self.rcl_services = self._to_dataframe(
            data=self._rcl_services,
            key='rcl_service_handle',
            columns=['rcl_node_handle', 'rmw_service_handle', 'service_name'],
        )

    def add_rcl_client(
        self, metadata: EventMetadata,
        rcl_client_handle: int, rcl_node_handle: int, rmw_client_handle: int, rcl_service_name: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_client_handle': rcl_client_handle,
            'rcl_node_handle': rcl_node_handle,
            'rmw_client_handle': rmw_client_handle,
            'rcl_service_name': rcl_service_name,
        }
        self._rcl_clients.append(obj)
        return obj

    def _finalize_rcl_clients(self):
        self.rcl_clients = self._to_dataframe(
            data=self._rcl_clients,
            key='rcl_client_handle',
            columns=['rcl_node_handle', 'rmw_client_handle', 'service_name'],
        )

    def add_rcl_timer(
        self, metadata: EventMetadata,
        rcl_timer_handle: int, period: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_timer_handle': rcl_timer_handle,
            'period': period,
        }
        self._rcl_timers.append(obj)
        return obj

    def _finalize_rcl_timers(self):
        self.rcl_timers = self._to_dataframe(
            data=self._rcl_timers,
            key='rcl_timer_handle',
            columns=['period'],
        )

    def add_timer_node_link(
        self, metadata: EventMetadata,
        rcl_timer_handle: int, rcl_node_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_timer_handle': rcl_timer_handle,
            'rcl_node_handle': rcl_node_handle,
        }
        self._timer_node_links.append(obj)
        return obj

    def _finalize_timer_node_links(self):
        self.timer_node_links = self._to_dataframe(
            data=self._timer_node_links,
            key='rcl_timer_handle',
            columns=['rcl_node_handle'],
        )

    def add_callback_object(
        self, metadata: EventMetadata,
        reference: int, callback_object: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'reference': reference,
            'callback_object': callback_object,
        }
        self._callback_objects.append(obj)
        return obj

    def _finalize_callback_objects(self):
        self.callback_objects = self._to_dataframe(
            data=self._callback_objects,
            key='callback_object',
            columns=['reference'],
        )

    def add_callback_symbol(
        self, metadata: EventMetadata,
        callback_object: int, symbol: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'callback_object': callback_object,
            'symbol': symbol,
        }
        self._callback_symbols.append(obj)
        return obj

    def _finalize_callback_symbols(self):
        self.callback_symbols = self._to_dataframe(
            data=self._callback_symbols,
            key='callback_object',
            columns=['symbol'],
        )

    def add_callback_instance(
        self, metadata: EventMetadata,
        callback_object: int, start_timestamp: int, duration: int, intra_process: bool,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'callback_object': callback_object,
            'start_timestamp': np.datetime64(start_timestamp, 'ns'),
            'duration': np.timedelta64(duration, 'ns'),
            'intra_process': intra_process,
        }
        self._callback_instances.append(obj)
        return obj

    def _finalize_callback_instances(self):
        self.callback_instances = pd.DataFrame(
            data=self._callback_instances,
            columns=[
                'timestamp', 'pid', 'tid', 'cpu_id',
                'callback_object', 'start_timestamp', 'duration', 'intra_process',
            ],
        )

    def add_rmw_take_instance(
        self, metadata: EventMetadata,
        rmw_subscription_handle: int, message_handle: int, source_timestamp: int, taken: bool,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rmw_subscription_handle': rmw_subscription_handle,
            'message_handle': message_handle,
            'source_timestamp': source_timestamp,
            'taken': taken,
        }
        self._rmw_take_instances.append(obj)
        return obj

    def _finalize_rmw_take_instances(self):
        self.rmw_take_instances = pd.DataFrame(
            data=self._rmw_take_instances,
            columns=[
                'timestamp', 'pid', 'tid', 'cpu_id',
                'rmw_subscription_handle', 'message_handle', 'source_timestamp', 'taken',
            ],
        )

    def add_rcl_take_instance(
        self, metadata: EventMetadata,
        message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'message_handle': message_handle,
        }
        self._rcl_take_instances.append(obj)
        return obj

    def _finalize_rcl_take_instances(self):
        self.rcl_take_instances = pd.DataFrame(
            data=self._rcl_take_instances,
            columns=[
                'timestamp', 'pid', 'tid', 'cpu_id',
                'message_handle',
            ],
        )

    def add_rclcpp_take_instance(
        self, metadata: EventMetadata,
        message_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'message_handle': message_handle,
        }
        self._rclcpp_take_instances.append(obj)
        return obj

    def _finalize_rclcpp_take_instances(self):
        self.rclcpp_take_instances = pd.DataFrame(
            data=self._rclcpp_take_instances,
            columns=[
                'timestamp', 'pid', 'tid', 'cpu_id',
                'message_handle',
            ],
        )

    def add_rcl_lifecycle_state_machine(
        self, metadata: EventMetadata,
        rcl_node_handle: int, rcl_state_machine_handle: int,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_node_handle': rcl_node_handle,
            'rcl_state_machine_handle': rcl_state_machine_handle,
        }
        self._lifecycle_state_machines.append(obj)
        return obj

    def _finalize_rcl_lifecycle_state_machines(self):
        self.rcl_lifecycle_state_machines = self._to_dataframe(
            data=self._lifecycle_state_machines,
            key='rcl_state_machine_handle',
            columns=['rcl_node_handle'],
        )

    def add_rcl_lifecycle_state_transition(
        self, metadata: EventMetadata,
        rcl_state_machine_handle: int, start_label: str, goal_label: str,
    ) -> Dict[str, Any]:
        obj = {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,

            'rcl_state_machine_handle': rcl_state_machine_handle,
            'start_label': start_label,
            'goal_label': goal_label,
        }
        self._rcl_lifecycle_transitions.append(obj)
        return obj

    def _finalize_rcl_lifecycle_transitions(self):
        self.rcl_lifecycle_transitions = pd.DataFrame(
            data=self._rcl_lifecycle_transitions,
            columns=[
                'timestamp', 'pid', 'tid', 'cpu_id',
                'rcl_state_machine_handle', 'start_label', 'goal_label',
            ],
        )

    def _finalize(self) -> None:
        self._finalize_dds_writers()
        self._finalize_dds_readers()
        self._finalize_dds_write_instances()
        self._finalize_dds_read_instances()
        self._finalize_rcl_contexts()
        self._finalize_rcl_nodes()
        self._finalize_rmw_publishers()
        self._finalize_rcl_publishers()
        self._finalize_rclcpp_publish_instances()
        self._finalize_rcl_publish_instances()
        self._finalize_rmw_publish_instances()
        self._finalize_rmw_subscriptions()
        self._finalize_rcl_subscriptions()
        self._finalize_rclcpp_subscriptions()
        self._finalize_rcl_services()
        self._finalize_rcl_clients()
        self._finalize_rcl_timers()
        self._finalize_timer_node_links()
        self._finalize_callback_objects()
        self._finalize_callback_symbols()
        self._finalize_callback_instances()
        self._finalize_rmw_take_instances()
        self._finalize_rcl_take_instances()
        self._finalize_rclcpp_take_instances()
        self._finalize_rcl_lifecycle_state_machines()
        self._finalize_rcl_lifecycle_transitions()

    def print_data(self) -> None:
        print('====================ROS 2 DATA MODEL===================')
        print('Contexts (rcl):')
        print(self.rcl_contexts.to_string())
        print()
        print('Nodes (rcl):')
        print(self.rcl_nodes.to_string())
        print()
        print('Publishers (rmw):')
        print(self.rmw_publishers.to_string())
        print()
        print('Publishers (rcl):')
        print(self.rcl_publishers.to_string())
        print()
        print('Subscriptions (rmw):')
        print(self.rmw_subscriptions.to_string())
        print()
        print('Subscriptions (rcl):')
        print(self.rcl_subscriptions.to_string())
        print()
        print('Subscriptions (rclcpp):')
        print(self.rclcpp_subscriptions.to_string())
        print()
        print('Services: (rcl)')
        print(self.rcl_services.to_string())
        print()
        print('Clients: (rcl)')
        print(self.rcl_clients.to_string())
        print()
        print('Timers: (rcl)')
        print(self.rcl_timers.to_string())
        print()
        print('Timer-node links:')
        print(self.timer_node_links.to_string())
        print()
        print('Callback objects:')
        print(self.callback_objects.to_string())
        print()
        print('Callback symbols:')
        print(self.callback_symbols.to_string())
        print()
        print('Callback instances:')
        print(self.callback_instances.to_string())
        print()
        print('Publish instances (rclcpp):')
        print(self.rclcpp_publish_instances.to_string())
        print()
        print('Publish instances (rcl):')
        print(self.rcl_publish_instances.to_string())
        print()
        print('Publish instances (rmw):')
        print(self.rmw_publish_instances.to_string())
        print()
        print('Take instances (rmw):')
        print(self.rmw_take_instances.to_string())
        print()
        print('Take instances (rcl):')
        print(self.rcl_take_instances.to_string())
        print()
        print('Take instances (rclcpp):')
        print(self.rclcpp_take_instances.to_string())
        print()
        print('Lifecycle state machines: (rcl)')
        print(self.rcl_lifecycle_state_machines.to_string())
        print()
        print('Lifecycle transitions: (rcl)')
        print(self.rcl_lifecycle_transitions.to_string())
        print('==================================================')
