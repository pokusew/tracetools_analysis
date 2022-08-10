# Copyright 2019 Robert Bosch GmbH
# Copyright 2020 Christophe Bedard
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

"""Module for trace events processor and ROS 2 model creation."""

from typing import Dict
from typing import Set
from typing import Tuple
from typing import List
from typing import Hashable
from typing import Any
from typing import Optional
from typing import Union
from typing import TypedDict

from tracetools_read import get_field

from . import EventHandler
from . import EventMetadata
from . import HandlerMap
from ..data_model.ros2 import Ros2DataModel, PID, LocalHandle, GlobalHandle, MessageHandle, CallbackHandle, MessageUID


class _RecordTypeIndexStats(TypedDict):
    overwrites: int
    misses: int


class _RecordType(TypedDict):
    name: str
    indexes: List[Union[str, Tuple[str, ...]]]
    maintain_list: bool
    data: List[Dict[str, Any]]
    indexed_by: Dict[Union[str, Tuple[str, ...]], Dict[Hashable, Dict[str, Any]]]
    index_stats: Dict[Union[str, Tuple[str, ...]], _RecordTypeIndexStats]


class UniquelyIndexedRecords:

    def __init__(self) -> None:
        self._records: Dict[str, _RecordType] = {}

    @staticmethod
    def get_index_value(index_name: Union[str, Tuple[str, ...]], data: Dict[str, Any]) -> Any:
        if isinstance(index_name, str):
            index_value = data[index_name]
        elif isinstance(index_name, tuple):
            index_value = tuple(data[key] for key in index_name)
            if None in index_value:
                return None
        else:
            raise RuntimeError(f'Invalid index of type {type(index_name)}')
        return index_value

    def register_type(self, name: str, indexes: List[Union[str, Tuple[str, ...]]], maintain_list: bool = False):
        record_def: _RecordType = {
            'name': name,
            'indexes': indexes,
            'maintain_list': maintain_list,
            'data': [],
            'indexed_by': {index_name: {} for index_name in indexes},
            'index_stats': {index_name: {'overwrites': 0, 'misses': 0} for index_name in indexes},
        }
        self._records[name] = record_def

    def add_record(self, name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        record_def = self._records[name]
        if record_def['maintain_list']:
            record_def['data'].append(data)
        for index_name in record_def['indexes']:
            index_value = self.get_index_value(index_name, data)
            if index_value is None:
                # skip for uniqueness
                continue
            if index_value in record_def['indexed_by'][index_name]:
                record_def['index_stats'][index_name]['overwrites'] += 1
            record_def['indexed_by'][index_name][index_value] = data
        return data

    def get_record_by(
        self, name: str, index_name: Union[str, Tuple[str, ...]], index_value: Hashable, remove: bool = False,
    ) -> Optional[Dict[str, Any]]:
        record_def = self._records[name]
        data = record_def['indexed_by'][index_name].get(index_value)
        if data is None:
            record_def['index_stats'][index_name]['misses'] += 1
        elif remove:
            if record_def['maintain_list']:
                record_def['data'].remove(data)
            # remove ref from all indexes
            for index_name in record_def['indexes']:
                index_value = self.get_index_value(index_name, data)
                if index_value is None:
                    # skip for uniqueness
                    continue
                if index_value in record_def['indexed_by'][index_name]:
                    del record_def['indexed_by'][index_name][index_value]
        return data

    def remove_record_by(
        self, name: str, index_name: Union[str, Tuple[str, ...]], index_value: Hashable,
    ) -> Optional[Dict[str, Any]]:
        return self.get_record_by(name=name, index_name=index_name, index_value=index_value, remove=True)

    def remove_record(
        self, name: str, data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        record_def = self._records[name]
        if data in record_def['data']:
            record_def['data'].remove(data)
        if len(record_def['indexed_by']) > 0:
            # try to use the first index_name and call remove_record_by
            for index_name in record_def['indexed_by'].keys():
                index_value = self.get_index_value(index_name, data)
                if index_value is None:
                    continue
                return self.remove_record_by(name=name, index_name=index_name, index_value=index_value)
            raise RuntimeError(
                'UniquelyIndexedRecords.remove_record: No index to remove by. This should never happen.'
            )
        return data

    def print_stats(self):
        for name, record_def in self._records.items():
            print(f'--- {name}:')
            list_count = len(record_def['data'])
            if list_count > 0 or record_def['maintain_list']:
                print(f' -C={list_count:4d}')
            for index_name, stats in record_def['index_stats'].items():
                o = stats['overwrites']
                m = stats['misses']
                count = len(record_def['indexed_by'][index_name])
                print(f'- O={o:4d} M={m:4d} C={count:4d} for {index_name}')
        pass

    def get_stats_for(
        self, name: str, index_name: Union[str, Tuple[str, ...]],
    ) -> _RecordTypeIndexStats:
        record_def = self._records[name]
        return record_def['index_stats'][index_name]

    def update_record_reindex_optional(
        self, name: str,
        index_name: Union[str, Tuple[str, ...]], index_value: Hashable,
        reindex_name: Union[str, Tuple[str, ...]]
    ):
        data = self.get_record_by(name=name, index_name=index_name, index_value=index_value)
        if data is None:
            return False
        record_def = self._records[name]
        reindex_value = self.get_index_value(reindex_name, data)
        if reindex_value is None:
            return False
        record_def['indexed_by'][reindex_name][reindex_value] = data
        return True


class Ros2Handler(EventHandler):
    """
    ROS 2 and DDS aware event handler that builds a full ROS 2 objects model with causal links.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:
        handler_map: HandlerMap = {
            'dds:create_writer':
                self._handle_dds_create_writer,
            'dds:create_reader':
                self._handle_dds_create_reader,
            'dds:write_pre':
                self._handle_dds_write_pre,
            'dds:write':
                self._handle_dds_write,
            'dds:read':
                self._handle_dds_read,
            'ros2:rcl_init':
                self._handle_rcl_init,
            'ros2:rcl_node_init':
                self._handle_rcl_node_init,
            'ros2:rmw_publisher_init':
                self._handle_rmw_publisher_init,
            'ros2:rcl_publisher_init':
                self._handle_rcl_publisher_init,
            'ros2:rclcpp_publish':
                self._handle_rclcpp_publish,
            'ros2:rcl_publish':
                self._handle_rcl_publish,
            'ros2:rmw_publish':
                self._handle_rmw_publish,
            'ros2:rmw_subscription_init':
                self._handle_rmw_subscription_init,
            'ros2:rcl_subscription_init':
                self._handle_rcl_subscription_init,
            'ros2:rclcpp_subscription_init':
                self._handle_rclcpp_subscription_init,
            'ros2:rclcpp_subscription_callback_added':
                self._handle_rclcpp_subscription_callback_added,
            'ros2:rmw_take':
                self._handle_rmw_take,
            'ros2:rcl_take':
                self._handle_rcl_take,
            'ros2:rclcpp_take':
                self._handle_rclcpp_take,
            'ros2:rcl_service_init':
                self._handle_rcl_service_init,
            'ros2:rclcpp_service_callback_added':
                self._handle_rclcpp_service_callback_added,
            'ros2:rcl_client_init':
                self._handle_rcl_client_init,
            'ros2:rcl_timer_init':
                self._handle_rcl_timer_init,
            'ros2:rclcpp_timer_callback_added':
                self._handle_rclcpp_timer_callback_added,
            'ros2:rclcpp_timer_link_node':
                self._handle_rclcpp_timer_link_node,
            'ros2:rclcpp_callback_register':
                self._handle_rclcpp_callback_register,
            'ros2:callback_start':
                self._handle_callback_start,
            'ros2:callback_end':
                self._handle_callback_end,
            'ros2:rcl_lifecycle_state_machine_init':
                self._handle_rcl_lifecycle_state_machine_init,
            'ros2:rcl_lifecycle_transition':
                self._handle_rcl_lifecycle_transition,
        }
        super().__init__(
            handler_map=handler_map,
            data_model=Ros2DataModel(),
            **kwargs,
        )

        self._records = UniquelyIndexedRecords()

        # Objects (one-time events, usually when something is created)

        # DDS writers are created for publishers and services (and actions) by RMW
        self._records.register_type(name='dds_writer', indexes=[('pid', 'gid')])
        # DDS readers are created for subscriptions and services (and actions) by RMW
        self._records.register_type(name='dds_reader', indexes=[('pid', 'gid')])

        self._records.register_type(name='rcl_context', indexes=[('pid', 'rcl_context_handle')])
        self._records.register_type(name='rcl_node', indexes=[('pid', 'rcl_node_handle')])

        # When a publisher is created, the sequence of events is:
        # 1. dds:create_writer -> 2. ros2:rmw_publisher_init -> 3. ros2:rcl_publisher_init
        # That means we should recreate a high-level subscription object after we parse 3.
        self._records.register_type(name='rmw_publisher', indexes=[('pid', 'rmw_publisher_handle')])
        self._records.register_type(name='rcl_publisher', indexes=[('pid', 'rcl_publisher_handle')])
        self._records.register_type(
            name='publisher',
            indexes=[
                ('pid', 'rcl_publisher_handle'),
                ('pid', 'rmw_publisher_handle'),
                ('pid', 'dds_writer_handle'),
            ],
            maintain_list=True,
        )

        # When a subscription is created, the sequence of events is:
        # 1. dds:create_reader -> 2. ros2:rmw_subscription_init -> 3. ros2:rcl_subscription_init
        # 4. -> rclcpp: ros2:rclcpp_subscription_init
        #    -> rclpy: not implemented in tracetools yet
        # That means we should recreate a high-level subscription object after we parse 3.
        # (so we have even when there is an unsupported client library).
        # If we find also 4., then we can add additional info to that object.
        self._records.register_type(name='rmw_subscription', indexes=[('pid', 'rmw_subscription_handle')])
        self._records.register_type(name='rcl_subscription', indexes=[('pid', 'rcl_subscription_handle')])
        self._records.register_type(name='rclcpp_subscription', indexes=[('pid', 'rcl_subscription_handle')])
        self._records.register_type(
            name='subscription',
            indexes=[
                ('pid', 'rclcpp_subscription_handle'),
                ('pid', 'rcl_subscription_handle'),
                ('pid', 'rmw_subscription_handle'),
                ('pid', 'dds_reader_handle'),
            ],
            maintain_list=True,
        )

        # Events (multiple instances, may not have a meaningful index)

        # publish flow: rclcpp/rclpy publish -> rcl publish -> rmw publish -> dds write
        self._records.register_type(name='rclcpp_publish_event', indexes=[('pid', 'message_handle')])
        self._records.register_type(name='rcl_publish_event', indexes=[('pid', 'message_handle')])
        self._records.register_type(name='rmw_publish_event', indexes=[('pid', 'message_handle')])
        self._records.register_type(name='dds_write_pre_event', indexes=[('pid', 'dds_writer_handle')])
        self._records.register_type(name='dds_write_event', indexes=[], maintain_list=True)
        self._records.register_type(name='publish_instance', indexes=[
            # message uid for matching publish instances with receptions in subscriptions (read -> take -> callback)
            # TODO: Switch to (message_timestamp, gid) once rmw_cyclonedds correctly supports gid it.
            # Note: Although the combination of message_timestamp and topic_name is virtually always unique,
            # in theory, there can be a situation (only on multi-core CPU) when two messages are published
            # by two different nodes to the same topic at the very same time (same nanosecond time).
            # To be really sure, we have an active assertion in finalize() to check for this rare occurrence.
            ('message_timestamp', 'topic_name')
        ], maintain_list=True)

        # subscription flow:
        # dds_read -> rmw_take -> rcl_take -> rclcpp_take
        self._records.register_type(name='dds_read_event', indexes=[], maintain_list=True)

        # TODO: migrate callback instances
        self._callback_start_events: Dict[CallbackHandle, Tuple[Dict, EventMetadata]] = {}

    @staticmethod
    def required_events() -> Set[str]:
        return {
            'ros2:rcl_init',
        }

    @property
    def data(self) -> Ros2DataModel:
        return super().data  # type: ignore

    @staticmethod
    def _dds_gid_to_rmw_gid(gid_prefix: Tuple[int, ...], gid_entity: Tuple[int, ...]) -> Tuple[int, ...]:
        assert len(gid_prefix) == 12
        assert len(gid_entity) == 4
        return tuple(gid_prefix + gid_entity + tuple(0 for _ in range(8)))

    @staticmethod
    def _extract_meta(metadata: EventMetadata) -> Dict[str, Any]:
        return {
            'timestamp': metadata.timestamp,
            'pid': metadata.pid,
            'tid': metadata.tid,
            'cpu_id': metadata.cpu_id,
        }

    def _handle_dds_create_writer(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        gid_prefix = tuple(get_field(event, 'gid_prefix'))
        gid_entity = tuple(get_field(event, 'gid_entity'))
        self._records.add_record('dds_writer', {
            **self._extract_meta(metadata),
            'dds_writer_handle': get_field(event, 'writer'),
            'dds_topic_name': get_field(event, 'topic_name'),
            'gid_prefix': gid_prefix,
            'gid_entity': gid_entity,
            'gid': self._dds_gid_to_rmw_gid(gid_prefix=gid_prefix, gid_entity=gid_entity)
        })

    def _handle_dds_create_reader(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        gid_prefix = tuple(get_field(event, 'gid_prefix'))
        gid_entity = tuple(get_field(event, 'gid_entity'))
        self._records.add_record('dds_reader', {
            **self._extract_meta(metadata),
            'dds_reader_handle': get_field(event, 'reader'),
            'dds_topic_name': get_field(event, 'topic_name'),
            'gid_prefix': gid_prefix,
            'gid_entity': gid_entity,
            'gid': self._dds_gid_to_rmw_gid(gid_prefix=gid_prefix, gid_entity=gid_entity)
        })

    def _handle_dds_write_pre(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('dds_write_pre_event', {
            **self._extract_meta(metadata),
            'dds_writer_handle': get_field(event, 'writer'),
            'message_handle': get_field(event, 'data'),
        })

    def _handle_dds_write(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        dds_writer_handle: LocalHandle = get_field(event, 'writer')

        # Currently, with the Fast DDS instrumentation, the dds:write event
        # does not contain the message/data pointer. We need to get it from
        # a separate previous event, dds:write_pre.
        message_handle = get_field(event, 'data', raise_if_not_found=False)

        message_timestamp = get_field(event, 'timestamp')

        # If we don't have the `data` field (message_handle) (in case of Fast DSS),
        # we try to get it from the corresponding dds:write_pre event.
        if message_handle is None:
            global_dds_writer_handle: GlobalHandle = (pid, dds_writer_handle)
            dds_write_pre_event = self._records.remove_record_by(
                name='dds_write_pre_event',
                index_name=('pid', 'dds_writer_handle'),
                index_value=global_dds_writer_handle,
            )
            if dds_write_pre_event is None:
                # there is nothing we can do, just log this situation
                # (our UniquelyIndexedRecords automatically logs index misses)
                return
            message_handle = dds_write_pre_event['message_handle']
            assert isinstance(message_handle, int)

        # add dds:write event with all fields in all cases
        dds_write_event = self._records.add_record('dds_write_event', {
            **self._extract_meta(metadata),
            'dds_writer_handle': dds_writer_handle,
            'message_handle': message_handle,
            'message_timestamp': message_timestamp,
        })

        self._reconstruct_publish_instance(dds_write_event=dds_write_event)

    def _reconstruct_publish_instance(self, dds_write_event: Dict[str, Any]):

        # lookup related rmw/rcl/rclcpp publish events

        pid = dds_write_event['pid']

        global_message_handle: MessageHandle = (pid, dds_write_event['message_handle'])

        rmw_publish_event = self._records.remove_record_by(
            name='rmw_publish_event',
            index_name=('pid', 'message_handle'),
            index_value=global_message_handle,
        )

        if rmw_publish_event is None:
            return

        rcl_publish_event = self._records.remove_record_by(
            name='rcl_publish_event',
            index_name=('pid', 'message_handle'),
            index_value=global_message_handle,
        )

        if rcl_publish_event is None:
            return

        rclcpp_publish_event = self._records.remove_record_by(
            name='rclcpp_publish_event',
            index_name=('pid', 'message_handle'),
            index_value=global_message_handle,
        )

        publisher = self._records.get_record_by(
            name='publisher',
            index_name=('pid', 'rcl_publisher_handle'),
            index_value=(pid, rcl_publish_event['rcl_publisher_handle']),
        )

        if publisher is None:
            return

        # TODO: better schema
        self._records.add_record('publish_instance', {
            **publisher,
            **rmw_publish_event,
            **rcl_publish_event,
            **dds_write_event,
        })

        self._records.remove_record('dds_write_event', dds_write_event)

    def _handle_dds_read(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('dds_read_event', {
            **self._extract_meta(metadata),
            'dds_reader_handle': get_field(event, 'reader'),
            'message_handle': get_field(event, 'buffer'),
        })

    def _handle_rcl_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('rcl_context', {
            **self._extract_meta(metadata),
            'rcl_context_handle': get_field(event, 'context_handle'),
            'tracetools_version': get_field(event, 'version'),
        })

    def _handle_rcl_node_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        # TODO: How to correctly associate rcl_context with rcl_node?
        #       Is it possible at all (given current tracepoints)?
        self._records.add_record('rcl_node', {
            **self._extract_meta(metadata),
            'rcl_node_handle': get_field(event, 'node_handle'),
            'rmw_node_handle': get_field(event, 'rmw_handle'),
            'name': get_field(event, 'node_name'),
            'namespace': get_field(event, 'namespace'),
        })

    def _handle_rmw_publisher_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_publisher_handle = get_field(event, 'rmw_publisher_handle')
        gid = tuple(get_field(event, 'gid'))
        assert len(gid) == 24
        self._records.add_record('rmw_publisher', {
            **self._extract_meta(metadata),
            'rmw_publisher_handle': rmw_publisher_handle,
            'gid': gid,
        })

    def _handle_rcl_publisher_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_publisher = self._records.add_record('rcl_publisher', {
            **self._extract_meta(metadata),
            'rcl_publisher_handle': get_field(event, 'publisher_handle'),
            'rcl_node_handle': get_field(event, 'node_handle'),
            'rmw_publisher_handle': get_field(event, 'rmw_publisher_handle'),
            'topic_name': get_field(event, 'topic_name'),
            'depth': get_field(event, 'queue_depth'),
        })
        self._create_publisher(rcl_publisher=rcl_publisher)

    def _create_publisher(self, rcl_publisher: Dict[str, Any]):
        pid = rcl_publisher['pid']
        rmw_publisher_handle = rcl_publisher['rmw_publisher_handle']

        rmw_publisher = self._records.remove_record_by(
            name='rmw_publisher',
            index_name=('pid', 'rmw_publisher_handle'),
            index_value=(pid, rmw_publisher_handle)
        )

        if rmw_publisher is None:
            return

        gid = rmw_publisher['gid']

        dds_writer = self._records.remove_record_by(
            name='dds_writer',
            index_name=('pid', 'gid'),
            index_value=(pid, gid)
        )

        if dds_writer is None:
            return

        # TODO: better schema
        self._records.add_record('publisher', {
            **dds_writer,
            **rmw_publisher,
            **rcl_publisher,
        })

        self._records.remove_record('rcl_publisher', rcl_publisher)

    def _handle_rclcpp_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('rclcpp_publish_event', {
            **self._extract_meta(metadata),
            'message_handle': get_field(event, 'message'),
        })

    def _handle_rcl_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('rcl_publish_event', {
            **self._extract_meta(metadata),
            'rcl_publisher_handle': get_field(event, 'publisher_handle'),
            'message_handle': get_field(event, 'message'),
        })

    def _handle_rmw_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        self._records.add_record('rmw_publish_event', {
            **self._extract_meta(metadata),
            'message_handle': get_field(event, 'message'),
        })

    def _handle_rmw_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        gid = tuple(get_field(event, 'gid'))
        assert len(gid) == 24
        self._records.add_record('rmw_subscription', {
            **self._extract_meta(metadata),
            'rmw_subscription_handle': rmw_subscription_handle,
            'gid': gid,
        })

    def _handle_rcl_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_subscription = self._records.add_record('rcl_subscription', {
            **self._extract_meta(metadata),
            'rcl_subscription_handle': get_field(event, 'subscription_handle'),
            'rcl_node_handle': get_field(event, 'node_handle'),
            'rmw_subscription_handle': get_field(event, 'rmw_subscription_handle'),
            'topic_name': get_field(event, 'topic_name'),
            'depth': get_field(event, 'queue_depth'),
        })
        self._create_subscription(rcl_subscription=rcl_subscription)

    def _create_subscription(self, rcl_subscription: Dict[str, Any]):
        pid = rcl_subscription['pid']
        rmw_subscription_handle = rcl_subscription['rmw_subscription_handle']

        rmw_subscription = self._records.remove_record_by(
            name='rmw_subscription',
            index_name=('pid', 'rmw_subscription_handle'),
            index_value=(pid, rmw_subscription_handle)
        )

        if rmw_subscription is None:
            return

        gid = rmw_subscription['gid']

        dds_reader = self._records.remove_record_by(
            name='dds_reader',
            index_name=('pid', 'gid'),
            index_value=(pid, gid)
        )

        if dds_reader is None:
            return

        # TODO: better schema
        self._records.add_record('subscription', {
            **dds_reader,
            **rmw_subscription,
            **rcl_subscription,
            'rclcpp_subscription_handle': None,
        })

        self._records.remove_record('rcl_subscription', rcl_subscription)

    def _handle_rclcpp_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rclcpp_subscription = self._records.add_record('rclcpp_subscription', {
            **self._extract_meta(metadata),
            'rclcpp_subscription_handle': get_field(event, 'subscription'),
            'rcl_subscription_handle': get_field(event, 'subscription_handle'),
        })
        self._add_rclcpp_data_to_subscription(rclcpp_subscription=rclcpp_subscription)

    def _add_rclcpp_data_to_subscription(self, rclcpp_subscription: Dict[str, Any]) -> None:
        pid = rclcpp_subscription['pid']
        rcl_subscription_handle = rclcpp_subscription['rcl_subscription_handle']
        subscription = self._records.get_record_by(
            name='subscription',
            index_name=('pid', 'rcl_subscription_handle'),
            index_value=(pid, rcl_subscription_handle),
        )
        if subscription is None:
            return
        rclcpp_subscription_handle = rclcpp_subscription['rclcpp_subscription_handle']
        subscription['rclcpp_subscription_handle'] = rclcpp_subscription_handle
        result = self._records.update_record_reindex_optional(
            name='subscription',
            index_name=('pid', 'rcl_subscription_handle'),
            index_value=(pid, rcl_subscription_handle),
            reindex_name=('pid', 'rclcpp_subscription_handle'),
        )
        assert result is True
        self._records.remove_record('rclcpp_subscription', rclcpp_subscription)
        pass

    def _handle_rclcpp_subscription_callback_added(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rclcpp_subscription_handle = get_field(event, 'subscription')
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(metadata, rclcpp_subscription_handle, callback_object)

    def _handle_rmw_take(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        message = get_field(event, 'message')
        source_timestamp = get_field(event, 'source_timestamp')
        taken = bool(get_field(event, 'taken'))
        self.data.add_rmw_take_instance(
            metadata, rmw_subscription_handle, message, source_timestamp, taken
        )

    def _handle_rcl_take(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        message = get_field(event, 'message')
        self.data.add_rcl_take_instance(metadata, message)

    def _handle_rclcpp_take(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        message = get_field(event, 'message')
        self.data.add_rclcpp_take_instance(metadata, message)

    def _handle_rcl_service_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_service_handle = get_field(event, 'service_handle')
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_service_handle = get_field(event, 'rmw_service_handle')
        rcl_service_name = get_field(event, 'service_name')
        self.data.add_rcl_service(metadata, rcl_service_handle, rcl_node_handle, rmw_service_handle, rcl_service_name)

    def _handle_rclcpp_service_callback_added(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_service_handle = get_field(event, 'service_handle')
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(metadata, rcl_service_handle, callback_object)

    def _handle_rcl_client_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_client_handle = get_field(event, 'client_handle')
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_client_handle = get_field(event, 'rmw_client_handle')
        rcl_service_name = get_field(event, 'service_name')
        self.data.add_rcl_client(metadata, rcl_client_handle, rcl_node_handle, rmw_client_handle, rcl_service_name)

    def _handle_rcl_timer_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_timer_handle = get_field(event, 'timer_handle')
        period = get_field(event, 'period')
        self.data.add_rcl_timer(metadata, rcl_timer_handle, period)

    def _handle_rclcpp_timer_callback_added(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_timer_handle = get_field(event, 'timer_handle')
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(metadata, rcl_timer_handle, callback_object)

    def _handle_rclcpp_timer_link_node(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_timer_handle = get_field(event, 'timer_handle')
        rcl_node_handle = get_field(event, 'node_handle')
        self.data.add_timer_node_link(metadata, rcl_timer_handle, rcl_node_handle)

    def _handle_rclcpp_callback_register(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        symbol = get_field(event, 'symbol')
        self.data.add_callback_symbol(metadata, callback_object, symbol)

    def _handle_callback_start(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        callback_handle: LocalHandle = get_field(event, 'callback')
        global_callback_handle: GlobalHandle = (pid, callback_handle)
        self._callback_start_events[global_callback_handle] = (event, metadata)

    def _handle_callback_end(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        callback_handle: LocalHandle = get_field(event, 'callback')
        global_callback_handle: GlobalHandle = (pid, callback_handle)

        callback_start_event = self._callback_start_events.get(global_callback_handle)

        if callback_start_event is None:
            # self._missing_callback_start_events += 1
            print(f'No matching callback start for callback object "{global_callback_handle}"')
            return

        (event_start, metadata_start) = callback_start_event
        del self._callback_start_events[global_callback_handle]

        duration = metadata.timestamp - metadata_start.timestamp
        is_intra_process = get_field(event_start, 'is_intra_process', raise_if_not_found=False)
        self.data.add_callback_instance(
            metadata,
            callback_handle,
            metadata_start.timestamp,
            duration,
            bool(is_intra_process),
        )

    def _handle_rcl_lifecycle_state_machine_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_node_handle = get_field(event, 'node_handle')
        rcl_state_machine_handle = get_field(event, 'state_machine')
        self.data.add_rcl_lifecycle_state_machine(metadata, rcl_node_handle, rcl_state_machine_handle)

    def _handle_rcl_lifecycle_transition(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_state_machine_handle = get_field(event, 'state_machine')
        start_label = get_field(event, 'start_label')
        goal_label = get_field(event, 'goal_label')
        self.data.add_rcl_lifecycle_state_transition(metadata, rcl_state_machine_handle, start_label, goal_label)

    def finalize(self) -> None:
        print(
            f'Ros2Handler stats:'
        )
        self._records.print_stats()
        # see the comment for self._records.register_type(name='publish_instance', ...) in __init__()
        assert self._records.get_stats_for(
            name='publish_instance',
            index_name=('message_timestamp', 'topic_name'),
        )['misses'] == 0
        super().finalize()
