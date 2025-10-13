#!/usr/bin/env python3

import os
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription, GroupAction
from launch.conditions import IfCondition
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node, PushRosNamespace
from ament_index_python.packages import get_package_share_directory
from launch_ros.substitutions import FindPackageShare as RosFindPackageShare


def generate_launch_description():
    # Get package share directory
    pkg_share = get_package_share_directory('dpgo_ros')
    
    # Declare launch arguments
    declare_num_robots_arg = DeclareLaunchArgument(
        'num_robots',
        default_value='5',
        description='Number of robots'
    )
    
    declare_g2o_dataset_arg = DeclareLaunchArgument(
        'g2o_dataset',
        default_value='sphere2500',
        description='G2O dataset name'
    )
    
    declare_debug_arg = DeclareLaunchArgument(
        'debug',
        default_value='true',
        description='Debug flag'
    )
    
    declare_verbose_arg = DeclareLaunchArgument(
        'verbose',
        default_value='true',
        description='Verbose flag'
    )
    
    declare_publish_iterate_arg = DeclareLaunchArgument(
        'publish_iterate',
        default_value='true',
        description='Publish iterate flag'
    )
    
    declare_rgd_stepsize_arg = DeclareLaunchArgument(
        'RGD_stepsize',
        default_value='0.2',
        description='RGD stepsize'
    )
    
    declare_rgd_use_preconditioner_arg = DeclareLaunchArgument(
        'RGD_use_preconditioner',
        default_value='true',
        description='RGD use preconditioner flag'
    )
    
    declare_asynchronous_rate_arg = DeclareLaunchArgument(
        'asynchronous_rate',
        default_value='100',
        description='Asynchronous rate'
    )
    
    declare_local_initialization_method_arg = DeclareLaunchArgument(
        'local_initialization_method',
        default_value='Chordal',
        description='Local initialization method'
    )
    
    declare_robot_names_file_arg = DeclareLaunchArgument(
        'robot_names_file',
        default_value=os.path.join(pkg_share, 'params', 'robot_names.yaml'),
        description='Robot names file'
    )
    
    declare_robot_measurements_file_arg = DeclareLaunchArgument(
        'robot_measurements_file',
        default_value=os.path.join(pkg_share, 'params', 'robot_measurements.yaml'),
        description='Robot measurements file'
    )

    # Dataset publisher node
    dataset_publisher_node = Node(
        package='dpgo_ros',
        executable='dpgo_ros_dataset_publisher_node',
        name='dataset_publisher',
        output='screen',
        parameters=[
            {'num_robots': LaunchConfiguration('num_robots')},
            {'g2o_file': PathJoinSubstitution([
                RosFindPackageShare('dpgo_ros'),
                'data',
                [LaunchConfiguration('g2o_dataset'), '.g2o']
            ])},
            LaunchConfiguration('robot_names_file')
        ]
    )

    # PGO Agent launch file path
    pgo_agent_launch_file = os.path.join(pkg_share, 'launch', 'PGOAgent.launch.py')

    # Create PGO agents for each robot
    pgo_agents = []
    for i in range(5):  # 5 robots as specified in num_robots default
        robot_group = GroupAction([
            PushRosNamespace(f'kimera{i}'),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(pgo_agent_launch_file),
                launch_arguments={
                    'agent_id': str(i),
                    'asynchronous': 'true',
                    'asynchronous_rate': LaunchConfiguration('asynchronous_rate'),
                    'num_robots': LaunchConfiguration('num_robots'),
                    'robot_names_file': LaunchConfiguration('robot_names_file'),
                    'debug': LaunchConfiguration('debug'),
                    'verbose': LaunchConfiguration('verbose'),
                    'publish_iterate': LaunchConfiguration('publish_iterate'),
                    'local_initialization_method': LaunchConfiguration('local_initialization_method'),
                    'RGD_stepsize': LaunchConfiguration('RGD_stepsize'),
                    'RGD_use_preconditioner': LaunchConfiguration('RGD_use_preconditioner'),
                    'synchronize_measurements': 'true',
                    'visualize_loop_closures': 'true',
                }.items()
            )
        ])
        pgo_agents.append(robot_group)

    # RViz node
    rviz_node = Node(
        package='rviz2',
        executable='rviz2',
        name='rviz',
        output='screen',
        arguments=['-d', os.path.join(pkg_share, 'rviz', 'default.rviz')]
    )

    return LaunchDescription([
        declare_num_robots_arg,
        declare_g2o_dataset_arg,
        declare_debug_arg,
        declare_verbose_arg,
        declare_publish_iterate_arg,
        declare_rgd_stepsize_arg,
        declare_rgd_use_preconditioner_arg,
        declare_asynchronous_rate_arg,
        declare_local_initialization_method_arg,
        declare_robot_names_file_arg,
        declare_robot_measurements_file_arg,
        dataset_publisher_node,
        *pgo_agents,
        rviz_node,
    ])