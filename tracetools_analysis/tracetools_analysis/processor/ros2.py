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
from typing import NewType

from tracetools_read import get_field

from . import EventHandler
from . import EventMetadata
from . import HandlerMap
from ..data_model.ros2 import Ros2DataModel, PID, LocalHandle, GlobalHandle, MessageHandle, CallbackHandle, MessageUID


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

        # integrity stats
        self._extra_rclcpp_publish_events: int = 0
        self._extra_rcl_publish_events: int = 0
        self._extra_rmw_publish_events: int = 0
        self._missing_rcl_publish_events: int = 0
        self._missing_rmw_publish_events: int = 0
        self._extra_dds_write_events: int = 0
        self._missing_dds_write_events: int = 0
        self._missing_callback_start_events: int = 0

        # dds:write_pre + dds_write events merging
        self._last_dds_pre_write_event_by_writer: Dict[GlobalHandle, Tuple[Dict, EventMetadata]] = {}

        # publish instances
        # rclcpp / rclpy -> rcl -> rmw -> dds
        self._last_rclcpp_publish_event_by_message: Dict[MessageHandle, Tuple[Dict, EventMetadata]] = {}
        self._last_rcl_publish_event_by_message: Dict[MessageHandle, Tuple[Dict, EventMetadata]] = {}
        self._last_rmw_publish_event_by_message: Dict[MessageHandle, Tuple[Dict, EventMetadata]] = {}

        # callback instances
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

    def _handle_dds_create_writer(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        writer_handle = get_field(event, 'writer')
        dds_topic_name = get_field(event, 'topic_name')
        gid_prefix = tuple(get_field(event, 'gid_prefix'))
        gid_entity = tuple(get_field(event, 'gid_entity'))
        gid = self._dds_gid_to_rmw_gid(gid_prefix=gid_prefix, gid_entity=gid_entity)
        self.data.add_dds_writer(metadata, writer_handle, dds_topic_name, gid_prefix, gid_entity, gid)

    def _handle_dds_create_reader(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        reader = get_field(event, 'reader')
        topic_name = get_field(event, 'topic_name')
        gid_prefix = tuple(get_field(event, 'gid_prefix'))
        gid_entity = tuple(get_field(event, 'gid_entity'))
        gid = self._dds_gid_to_rmw_gid(gid_prefix=gid_prefix, gid_entity=gid_entity)
        self.data.add_dds_reader(metadata, reader, topic_name, gid_prefix, gid_entity, gid)

    def _handle_dds_write_pre(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        writer_handle: LocalHandle = get_field(event, 'writer')
        global_writer_handle: GlobalHandle = (pid, writer_handle)
        if global_writer_handle in self._last_dds_pre_write_event_by_writer:
            self._extra_dds_write_events += 1
            # print(f'extra dds:write_pre event for writer {global_writer_handle} -> overwriting')
        self._last_dds_pre_write_event_by_writer[global_writer_handle] = (event, metadata)

    def _handle_dds_write(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        writer_handle: LocalHandle = get_field(event, 'writer')

        # Currently, with the Fast DDS instrumentation, the dds:write event
        # does not contain the message/data pointer. We need to get it from
        # a separate previous event, dds:write_pre.
        data = get_field(event, 'data', raise_if_not_found=False)

        msg_timestamp = get_field(event, 'timestamp')

        # if we don't have data fields (in case of Fast DSS),
        # try to get the data pointer from dds:write_pre event
        if data is None:
            global_writer_handle: GlobalHandle = (pid, writer_handle)
            dds_write_pre_event = self._last_dds_pre_write_event_by_writer.get(global_writer_handle)
            if dds_write_pre_event is None:
                self._missing_dds_write_events += 1
                # print(
                #     'missing corresponding dds:write_pre event for dds:write event'
                #     f' for writer {global_writer_handle} at {metadata.timestamp}'
                # )
                return
            dds_write_pre_event_e, _ = self._last_dds_pre_write_event_by_writer[global_writer_handle]
            del self._last_dds_pre_write_event_by_writer[global_writer_handle]
            data = get_field(dds_write_pre_event_e, 'data')

        # add dds:write event with all fields in all cases
        self.data.add_dds_write_instance(metadata, writer_handle, data, msg_timestamp)

        # lookup related rmw/rcl/rclcpp publish events

        global_message_handle: MessageHandle = (pid, data)

        rmw_publish_event = self._last_rmw_publish_event_by_message.get(global_message_handle)
        if rmw_publish_event is None:
            self._missing_rmw_publish_events += 1
            return
        del self._last_rmw_publish_event_by_message[global_message_handle]

        rcl_publish_event = self._last_rcl_publish_event_by_message.get(global_message_handle)
        if rcl_publish_event is None:
            self._missing_rcl_publish_events += 1
            return
        del self._last_rcl_publish_event_by_message[global_message_handle]

        rclcpp_publish_event = self._last_rclcpp_publish_event_by_message.get(global_message_handle)
        if rclcpp_publish_event is not None:
            del self._last_rclcpp_publish_event_by_message[global_message_handle]

        pass

    def _handle_dds_read(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        reader = get_field(event, 'reader')
        buffer = get_field(event, 'buffer')
        self.data.add_dds_read_instance(metadata, reader, buffer)

    def _handle_rcl_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_context_handle = get_field(event, 'context_handle')
        tracetools_version = get_field(event, 'version')
        self.data.add_rcl_context(metadata, rcl_context_handle, tracetools_version)

    def _handle_rcl_node_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_node_handle = get_field(event, 'rmw_handle')
        name = get_field(event, 'node_name')
        namespace = get_field(event, 'namespace')
        self.data.add_rcl_node(metadata, rcl_node_handle, rmw_node_handle, name, namespace)

    def _handle_rmw_publisher_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_publisher_handle = get_field(event, 'rmw_publisher_handle')
        gid = tuple(get_field(event, 'gid'))
        assert len(gid) == 24
        self.data.add_rmw_publisher(metadata, rmw_publisher_handle, gid)

    def _handle_rcl_publisher_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_publisher_handle = get_field(event, 'publisher_handle')
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_publisher_handle = get_field(event, 'rmw_publisher_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')
        self.data.add_rcl_publisher(
            metadata, rcl_publisher_handle, rcl_node_handle, rmw_publisher_handle, topic_name, depth
        )

    def _create_publisher(self, rcl_publisher_handle: GlobalHandle):
        pass

    def _add_rclcpp_data_to_subscription(self, rcl_publisher_handle: GlobalHandle):
        pass

    def _handle_rclcpp_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        message_handle = get_field(event, 'message')
        self.data.add_rclcpp_publish_instance(metadata, message_handle)
        global_message_handle: GlobalHandle = (pid, message_handle)
        if global_message_handle in self._last_rclcpp_publish_event_by_message:
            self._extra_rclcpp_publish_events += 1
            print(f'unexpected rclcpp_publish for message f{global_message_handle} -> overwriting')
        self._last_rclcpp_publish_event_by_message[global_message_handle] = (event, metadata)

    def _handle_rcl_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        rcl_publisher_handle = get_field(event, 'publisher_handle')
        message_handle = get_field(event, 'message')
        self.data.add_rcl_publish_instance(metadata, rcl_publisher_handle, message_handle)
        global_message_handle: GlobalHandle = (pid, message_handle)
        if global_message_handle in self._last_rcl_publish_event_by_message:
            self._extra_rcl_publish_events += 1
            print(f'unexpected rcl_publish for message f{global_message_handle} -> overwriting')
        self._last_rcl_publish_event_by_message[global_message_handle] = (event, metadata)

    def _handle_rmw_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        pid: PID = metadata.pid
        message_handle = get_field(event, 'message')
        self.data.add_rmw_publish_instance(metadata, message_handle)
        global_message_handle: GlobalHandle = (pid, message_handle)
        if global_message_handle in self._last_rmw_publish_event_by_message:
            self._extra_rmw_publish_events += 1
            print(f'unexpected rmw_publish for message f{global_message_handle} -> overwriting')
        self._last_rmw_publish_event_by_message[global_message_handle] = (event, metadata)

    def _handle_rmw_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        gid = tuple(get_field(event, 'gid'))
        assert len(gid) == 24
        self.data.add_rmw_subscription(metadata, rmw_subscription_handle, gid)

    def _handle_rcl_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_subscription_handle = get_field(event, 'subscription_handle')
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')
        self.data.add_rcl_subscription(
            metadata, rcl_subscription_handle, rcl_node_handle, rmw_subscription_handle, topic_name, depth,
        )

    def _handle_rclcpp_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rclcpp_subscription_handle = get_field(event, 'subscription')
        rcl_subscription_handle = get_field(event, 'subscription_handle')
        self.data.add_rclcpp_subscription(metadata, rclcpp_subscription_handle, rcl_subscription_handle)

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
            self._missing_callback_start_events += 1
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
            f'\n\t- extra_dds_write_events: {self._extra_dds_write_events}'
            f'\n\t- missing_dds_write_events: {self._missing_dds_write_events}'
        )
        super().finalize()
