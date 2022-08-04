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

from tracetools_read import get_field

from . import EventHandler
from . import EventMetadata
from . import HandlerMap
from ..data_model.ros2 import Ros2DataModel


class Ros2Handler(EventHandler):
    """
    ROS 2-aware event handling class implementation.

    Handles a trace's events and builds a model with the data.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:
        """Create a Ros2Handler."""
        # Link a ROS trace event to its corresponding handling method
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

        # Temporary buffers
        # callbacks are uniquely indexed by a tuple (pid, callback memory address)
        self._callback_instances: Dict[Tuple[int, int], Tuple[Dict, EventMetadata]] = {}

    @staticmethod
    def required_events() -> Set[str]:
        return {
            'ros2:rcl_init',
        }

    @property
    def data(self) -> Ros2DataModel:
        return super().data  # type: ignore

    def _handle_dds_create_writer(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        writer = get_field(event, 'writer')
        topic_name = get_field(event, 'topic_name')
        gid_prefix = get_field(event, 'gid_prefix')
        gid_entity = get_field(event, 'gid_entity')
        self.data.add_dds_writer(metadata, writer, topic_name, gid_prefix, gid_entity)

    def _handle_dds_create_reader(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        reader = get_field(event, 'reader')
        topic_name = get_field(event, 'topic_name')
        gid_prefix = get_field(event, 'gid_prefix')
        gid_entity = get_field(event, 'gid_entity')
        self.data.add_dds_reader(metadata, reader, topic_name, gid_prefix, gid_entity)

    def _handle_dds_write_pre(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        writer = get_field(event, 'writer')
        data = get_field(event, 'data')
        self.data.add_dds_write_pre_instance(metadata, writer, data)

    def _handle_dds_write(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        writer = get_field(event, 'writer')
        msg_timestamp = get_field(event, 'timestamp')
        self.data.add_dds_write_instance(metadata, writer, msg_timestamp)

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
        self.data.add_context(metadata, rcl_context_handle, tracetools_version)

    def _handle_rcl_node_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_node_handle = get_field(event, 'node_handle')
        rmw_node_handle = get_field(event, 'rmw_handle')
        name = get_field(event, 'node_name')
        namespace = get_field(event, 'namespace')
        self.data.add_node(metadata, rcl_node_handle, rmw_node_handle, name, namespace)

    def _handle_rmw_publisher_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_publisher_handle = get_field(event, 'rmw_publisher_handle')
        gid = get_field(event, 'gid')
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

    def _handle_rclcpp_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        message = get_field(event, 'message')
        self.data.add_rclcpp_publish_instance(metadata, message)

    def _handle_rcl_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_publisher_handle = get_field(event, 'publisher_handle')
        message = get_field(event, 'message')
        self.data.add_rcl_publish_instance(metadata, rcl_publisher_handle, message)

    def _handle_rmw_publish(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        message = get_field(event, 'message')
        self.data.add_rmw_publish_instance(metadata, message)

    def _handle_rmw_subscription_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        gid = get_field(event, 'gid')
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
        service_name = get_field(event, 'service_name')
        self.data.add_service(metadata, rcl_service_handle, rcl_node_handle, rmw_service_handle, service_name)

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
        service_name = get_field(event, 'service_name')
        self.data.add_client(metadata, rcl_client_handle, rcl_node_handle, rmw_client_handle, service_name)

    def _handle_rcl_timer_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_timer_handle = get_field(event, 'timer_handle')
        period = get_field(event, 'period')
        self.data.add_timer(metadata, rcl_timer_handle, period)

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
        callback_object = get_field(event, 'callback')
        callback_uid = (metadata.pid, callback_object)
        self._callback_instances[callback_uid] = (event, metadata)

    def _handle_callback_end(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        callback_uid = (metadata.pid, callback_object)
        callback_instance_data = self._callback_instances.get(callback_uid)
        if callback_instance_data is not None:
            (event_start, metadata_start) = callback_instance_data
            del self._callback_instances[callback_uid]
            duration = metadata.timestamp - metadata_start.timestamp
            is_intra_process = get_field(event_start, 'is_intra_process', raise_if_not_found=False)
            self.data.add_callback_instance(
                metadata,
                callback_object,
                metadata_start.timestamp,
                duration,
                bool(is_intra_process)
            )
        else:
            print(f'No matching callback start for callback object "{callback_object}"')

    def _handle_rcl_lifecycle_state_machine_init(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_node_handle = get_field(event, 'node_handle')
        rcl_state_machine_handle = get_field(event, 'state_machine')
        self.data.add_lifecycle_state_machine(metadata, rcl_node_handle, rcl_state_machine_handle)

    def _handle_rcl_lifecycle_transition(
        self, event: Dict, metadata: EventMetadata,
    ) -> None:
        rcl_state_machine_handle = get_field(event, 'state_machine')
        start_label = get_field(event, 'start_label')
        goal_label = get_field(event, 'goal_label')
        self.data.add_lifecycle_state_transition(metadata, rcl_state_machine_handle, start_label, goal_label)
