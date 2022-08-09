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

from typing import Dict, List, TypedDict, Mapping
from typing import Set
from typing import Tuple
from typing import NewType

from tracetools_read import get_field

import pandas as pd

from . import EventHandler
from . import EventMetadata
from . import HandlerMap
from ..data_model.ros2 import Ros2DataModel


class Ros2ModelHelper:
    """
    ROS 2 and DDS aware event handler that build a full ROS 2 objects model with causal links.
    """

    def __init__(
        self,
        model: Ros2DataModel,
    ) -> None:
        self._model: Ros2DataModel = model

        self._create_publishers()
        self._create_subscriptions()
        pass

    def _create_publishers(self):
        # prepare dataframes
        rcl_publishers = self._model.rcl_publishers.reset_index()
        rmw_publishers = self._model.rmw_publishers.reset_index()
        # some low-level rmw publishers do not have rcl equivalents
        # (for example rmw publishers responsible for discovery using DDS topic ros_discovery_info)
        assert len(rcl_publishers) < len(rmw_publishers)
        dds_writers = self._model.dds_writers.reset_index()
        # some DDS writers might be associated with services/actions
        assert len(dds_writers) >= len(rmw_publishers)

        # merge rcl + rmw + dds
        rcl_publishers_with_rmw = pd.merge(
            left=rcl_publishers,
            right=rmw_publishers,
            how='inner',
            on=['pid', 'rmw_publisher_handle'],
            validate='one_to_one',
            suffixes=('_rcl', '_rmw'),
        )
        assert len(rcl_publishers_with_rmw) == len(rcl_publishers)
        rcl_publishers_with_rmw_and_dds = pd.merge(
            left=rcl_publishers_with_rmw,
            right=dds_writers,
            how='inner',
            on=['pid', 'gid'],
            validate='one_to_one',
            suffixes=('_rmw', '_dds'),
        )
        # needed, because during merge above the following right column names will be unique
        # and left suffix won't be applied
        rcl_publishers_with_rmw_and_dds.rename(
            columns={
                'timestamp': 'timestamp_dds',
                'tid': 'tid_dds',
                'cpu_id': 'cpu_id_dds',
            },
            inplace=True,
        )
        assert len(rcl_publishers_with_rmw_and_dds) == len(rcl_publishers_with_rmw)

        # no need to handle rclcpp or rclpy here
        # as we currently don't collect any info from rclcpp or rclpy

        self._publishers = rcl_publishers_with_rmw_and_dds

    def _create_subscriptions(self):
        # prepare dataframes
        rcl_subscriptions = self._model.rcl_subscriptions.reset_index()
        rmw_subscriptions = self._model.rmw_subscriptions.reset_index()
        # some low-level rmw subscriptions do not have rcl equivalents
        # (for example rmw subscriptions responsible for discovery using DDS topic ros_discovery_info)
        assert len(rcl_subscriptions) < len(rmw_subscriptions)
        dds_readers = self._model.dds_readers.reset_index()
        # some DDS readers might be associated with services/actions
        assert len(dds_readers) >= len(rmw_subscriptions)

        # merge rcl + rmw + dds
        rcl_subscriptions_with_rmw = pd.merge(
            left=rcl_subscriptions,
            right=rmw_subscriptions,
            how='inner',
            on=['pid', 'rmw_subscription_handle'],
            validate='one_to_one',
            suffixes=('_rcl', '_rmw'),
        )
        assert len(rcl_subscriptions_with_rmw) == len(rcl_subscriptions)
        rcl_subscriptions_with_rmw_and_dds = pd.merge(
            left=rcl_subscriptions_with_rmw,
            right=dds_readers,
            how='inner',
            on=['pid', 'gid'],
            validate='one_to_one',
            suffixes=('_rmw', '_dds'),
        )
        # needed, because during merge above the following right column names will be unique
        # and left suffix won't be applied
        rcl_subscriptions_with_rmw_and_dds.rename(
            columns={
                'timestamp': 'timestamp_dds',
                'tid': 'tid_dds',
                'cpu_id': 'cpu_id_dds',
            },
            inplace=True,
        )
        assert len(rcl_subscriptions_with_rmw_and_dds) == len(rcl_subscriptions_with_rmw)

        # add info from higher levels - rclcpp
        rclcpp_subscriptions = self._model.rclcpp_subscriptions.reset_index()
        subscriptions_with_rclcpp = pd.merge(
            left=rcl_subscriptions_with_rmw_and_dds,
            right=rclcpp_subscriptions,
            how='left',
            on=['pid', 'rcl_subscription_handle'],
            validate='one_to_one',
            suffixes=('_rcl', '_rclcpp'),
        )
        # needed, because during merge above the following right column names will be unique
        # and left suffix won't be applied
        subscriptions_with_rclcpp.rename(
            columns={
                'timestamp': 'timestamp_rclcpp',
                'tid': 'tid_rclcpp',
                'cpu_id': 'cpu_id_rclcpp',
            },
            inplace=True,
        )
        assert len(subscriptions_with_rclcpp) == len(subscriptions_with_rclcpp)

        # TODO: add rclpy here once it is supported

        self._subscriptions = subscriptions_with_rclcpp
