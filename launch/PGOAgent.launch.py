#!/usr/bin/env python3

import os
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.conditions import IfCondition, UnlessCondition
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare as RosFindPackageShare
from ament_index_python.packages import get_package_share_directory

def launch_setup(context, *args, **kwargs):
    """Convert string parameters to appropriate types"""
    
    # Get parameter values and convert to appropriate types
    agent_id = int(LaunchConfiguration('agent_id').perform(context))
    num_robots = int(LaunchConfiguration('num_robots').perform(context))
    dimension = int(LaunchConfiguration('dimension').perform(context))
    relaxation_rank = int(LaunchConfiguration('relaxation_rank').perform(context))
    asynchronous = LaunchConfiguration('asynchronous').perform(context).lower() == 'true'
    asynchronous_rate = float(LaunchConfiguration('asynchronous_rate').perform(context))
    rtr_iterations = int(LaunchConfiguration('RTR_iterations').perform(context))
    rtr_tcg_iterations = int(LaunchConfiguration('RTR_tCG_iterations').perform(context))
    rtr_gradnorm_tol = float(LaunchConfiguration('RTR_gradnorm_tol').perform(context))
    rgd_stepsize = float(LaunchConfiguration('RGD_stepsize').perform(context))
    rgd_use_preconditioner = LaunchConfiguration('RGD_use_preconditioner').perform(context).lower() == 'true'
    acceleration = LaunchConfiguration('acceleration').perform(context).lower() == 'true'
    restart_interval = int(LaunchConfiguration('restart_interval').perform(context))
    gnc_use_probability = LaunchConfiguration('GNC_use_probability').perform(context).lower() == 'true'
    gnc_quantile = float(LaunchConfiguration('GNC_quantile').perform(context))
    gnc_barc = float(LaunchConfiguration('GNC_barc').perform(context))
    gnc_mu_step = float(LaunchConfiguration('GNC_mu_step').perform(context))
    gnc_init_mu = float(LaunchConfiguration('GNC_init_mu').perform(context))
    robust_opt_num_weight_updates = int(LaunchConfiguration('robust_opt_num_weight_updates').perform(context))
    robust_opt_num_resets = int(LaunchConfiguration('robust_opt_num_resets').perform(context))
    robust_opt_min_convergence_ratio = float(LaunchConfiguration('robust_opt_min_convergence_ratio').perform(context))
    robust_opt_inner_iters_per_robot = int(LaunchConfiguration('robust_opt_inner_iters_per_robot').perform(context))
    robust_init_min_inliers = int(LaunchConfiguration('robust_init_min_inliers').perform(context))
    verbose = LaunchConfiguration('verbose').perform(context).lower() == 'true'
    max_iteration_number = int(LaunchConfiguration('max_iteration_number').perform(context))
    relative_change_tolerance = float(LaunchConfiguration('relative_change_tolerance').perform(context))
    publish_iterate = LaunchConfiguration('publish_iterate').perform(context).lower() == 'true'
    visualize_loop_closures = LaunchConfiguration('visualize_loop_closures').perform(context).lower() == 'true'
    complete_reset = LaunchConfiguration('complete_reset').perform(context).lower() == 'true'
    enable_recovery = LaunchConfiguration('enable_recovery').perform(context).lower() == 'true'
    synchronize_measurements = LaunchConfiguration('synchronize_measurements').perform(context).lower() == 'true'
    max_distributed_init_steps = int(LaunchConfiguration('max_distributed_init_steps').perform(context))
    inter_update_sleep_time = int(LaunchConfiguration('inter_update_sleep_time').perform(context))
    weight_convergence_threshold = float(LaunchConfiguration('weight_convergence_threshold').perform(context))
    max_delayed_iterations = int(LaunchConfiguration('max_delayed_iterations').perform(context))
    timeout_threshold = int(LaunchConfiguration('timeout_threshold').perform(context))
    
    # Get string parameters
    update_rule = LaunchConfiguration('update_rule').perform(context)
    local_initialization_method = LaunchConfiguration('local_initialization_method').perform(context)
    multirobot_initialization = LaunchConfiguration('multirobot_initialization').perform(context).lower() == 'true'
    robust_cost_type = LaunchConfiguration('robust_cost_type').perform(context)
    log_output_path = LaunchConfiguration('log_directory').perform(context)
    robot_names_file = LaunchConfiguration('robot_names_file').perform(context)

    # PGO Agent node
    pgo_agent_node = Node(
        package='dpgo_ros',
        executable='dpgo_ros_node',
        namespace='dpgo_ros_node',
        name='agent',
        output='screen',
        parameters=[
            {'agent_id': agent_id},
            {'num_robots': num_robots},
            {'dimension': dimension},
            {'relaxation_rank': relaxation_rank},
            {'asynchronous': asynchronous},
            {'asynchronous_rate': asynchronous_rate},
            {'update_rule': update_rule},
            {'local_initialization_method': local_initialization_method},
            {'multirobot_initialization': multirobot_initialization},
            {'RGD_stepsize': rgd_stepsize},
            {'RGD_use_preconditioner': rgd_use_preconditioner},
            {'RTR_iterations': rtr_iterations},
            {'RTR_tCG_iterations': rtr_tcg_iterations},
            {'RTR_gradnorm_tol': rtr_gradnorm_tol},
            {'acceleration': acceleration},
            {'restart_interval': restart_interval},
            {'robust_cost_type': robust_cost_type},
            {'GNC_use_probability': gnc_use_probability},
            {'GNC_quantile': gnc_quantile},
            {'GNC_barc': gnc_barc},
            {'GNC_mu_step': gnc_mu_step},
            {'GNC_init_mu': gnc_init_mu},
            {'robust_opt_num_weight_updates': robust_opt_num_weight_updates},
            {'robust_opt_num_resets': robust_opt_num_resets},
            {'robust_opt_min_convergence_ratio': robust_opt_min_convergence_ratio},
            {'robust_opt_inner_iters_per_robot': robust_opt_inner_iters_per_robot},
            {'robust_init_min_inliers': robust_init_min_inliers},
            {'verbose': verbose},
            {'max_iteration_number': max_iteration_number},
            {'relative_change_tolerance': relative_change_tolerance},
            {'publish_iterate': publish_iterate},
            {'visualize_loop_closures': visualize_loop_closures},
            {'complete_reset': complete_reset},
            {'enable_recovery': enable_recovery},
            {'synchronize_measurements': synchronize_measurements},
            {'max_distributed_init_steps': max_distributed_init_steps},
            {'inter_update_sleep_time': inter_update_sleep_time},
            {'weight_convergence_threshold': weight_convergence_threshold},
            {'max_delayed_iterations': max_delayed_iterations},
            {'timeout_threshold': timeout_threshold},
            {'log_output_path': log_output_path},
            robot_names_file
        ],
        arguments=['--ros-args', '--log-level', 'INFO']
    )

    return [pgo_agent_node]
    
def generate_launch_description():
    # Declare launch arguments
    declare_debug_arg = DeclareLaunchArgument(
        'debug',
        default_value='false',
        description='Enable debugging'
    )
    
    declare_agent_id_arg = DeclareLaunchArgument(
        'agent_id', 
        default_value='0',
        description='Agent ID'
    )
    
    declare_num_robots_arg = DeclareLaunchArgument(
        'num_robots',
        default_value='1',
        description='Number of robots'
    )
    
    declare_dimension_arg = DeclareLaunchArgument(
        'dimension',
        default_value='3',
        description='Problem dimension'
    )
    
    declare_relaxation_rank_arg = DeclareLaunchArgument(
        'relaxation_rank',
        default_value='5',
        description='Relaxation rank'
    )
    
    declare_asynchronous_arg = DeclareLaunchArgument(
        'asynchronous',
        default_value='false',
        description='Enable asynchronous mode'
    )
    
    declare_asynchronous_rate_arg = DeclareLaunchArgument(
        'asynchronous_rate',
        default_value='100',
        description='Asynchronous rate'
    )
    
    declare_verbose_arg = DeclareLaunchArgument(
        'verbose',
        default_value='false',
        description='Enable verbose output'
    )
    
    declare_rgd_stepsize_arg = DeclareLaunchArgument(
        'RGD_stepsize',
        default_value='1e-4',
        description='RGD step size'
    )
    
    declare_rgd_use_preconditioner_arg = DeclareLaunchArgument(
        'RGD_use_preconditioner',
        default_value='false',
        description='Use RGD preconditioner'
    )
    
    declare_rtr_iterations_arg = DeclareLaunchArgument(
        'RTR_iterations',
        default_value='5',
        description='RTR iterations'
    )
    
    declare_rtr_tcg_iterations_arg = DeclareLaunchArgument(
        'RTR_tCG_iterations',
        default_value='5',
        description='RTR tCG iterations'
    )
    
    declare_rtr_gradnorm_tol_arg = DeclareLaunchArgument(
        'RTR_gradnorm_tol',
        default_value='1e-6',
        description='RTR gradient norm tolerance'
    )
    
    declare_local_initialization_method_arg = DeclareLaunchArgument(
        'local_initialization_method',
        default_value='given',
        description='Local initialization method'
    )
    
    declare_update_rule_arg = DeclareLaunchArgument(
        'update_rule',
        default_value='RoundRobin',
        description='Update rule'
    )
    
    declare_multirobot_initialization_arg = DeclareLaunchArgument(
        'multirobot_initialization',
        default_value='true',
        description='Multi robot initialization'
    )
    
    declare_acceleration_arg = DeclareLaunchArgument(
        'acceleration',
        default_value='false',
        description='Enable acceleration'
    )
    
    declare_restart_interval_arg = DeclareLaunchArgument(
        'restart_interval',
        default_value='1',
        description='Restart interval'
    )
    
    declare_robust_cost_type_arg = DeclareLaunchArgument(
        'robust_cost_type',
        default_value='Huber',
        description='Robust cost type'
    )
    
    declare_gnc_use_probability_arg = DeclareLaunchArgument(
        'GNC_use_probability',
        default_value='false',
        description='GNC use probability'
    )
    
    declare_gnc_quantile_arg = DeclareLaunchArgument(
        'GNC_quantile',
        default_value='0.5',
        description='GNC quantile'
    )
    
    declare_gnc_barc_arg = DeclareLaunchArgument(
        'GNC_barc',
        default_value='1.0',
        description='GNC barc'
    )
    
    declare_gnc_mu_step_arg = DeclareLaunchArgument(
        'GNC_mu_step',
        default_value='1.4',
        description='GNC mu step'
    )
    
    declare_gnc_init_mu_arg = DeclareLaunchArgument(
        'GNC_init_mu',
        default_value='1.0',
        description='GNC initial mu'
    )
    
    declare_robust_opt_num_weight_updates_arg = DeclareLaunchArgument(
        'robust_opt_num_weight_updates',
        default_value='1',
        description='Number of weight updates'
    )
    
    declare_robust_opt_num_resets_arg = DeclareLaunchArgument(
        'robust_opt_num_resets',
        default_value='0',
        description='Number of resets'
    )
    
    declare_robust_opt_min_convergence_ratio_arg = DeclareLaunchArgument(
        'robust_opt_min_convergence_ratio',
        default_value='1e-2',
        description='Minimum convergence ratio'
    )
    
    declare_robust_opt_inner_iters_per_robot_arg = DeclareLaunchArgument(
        'robust_opt_inner_iters_per_robot',
        default_value='1',
        description='Inner iterations per robot'
    )
    
    declare_robust_init_min_inliers_arg = DeclareLaunchArgument(
        'robust_init_min_inliers',
        default_value='100',
        description='Minimum inliers for robust initialization'
    )
    
    declare_max_iteration_number_arg = DeclareLaunchArgument(
        'max_iteration_number',
        default_value='100',
        description='Maximum iteration number'
    )
    
    declare_relative_change_tolerance_arg = DeclareLaunchArgument(
        'relative_change_tolerance',
        default_value='1e-7',
        description='Relative change tolerance'
    )
    
    declare_log_directory_arg = DeclareLaunchArgument(
        'log_directory',
        default_value='/tmp',
        description='Log directory'
    )
    
    declare_robot_names_file_arg = DeclareLaunchArgument(
        'robot_names_file',
        default_value=os.path.join(get_package_share_directory('dpgo_ros'), 'params', 'robot_names.yaml'),
        description='Robot names file'
    )
    
    declare_publish_iterate_arg = DeclareLaunchArgument(
        'publish_iterate',
        default_value='false',
        description='Publish iterate'
    )
    
    declare_visualize_loop_closures_arg = DeclareLaunchArgument(
        'visualize_loop_closures',
        default_value='false',
        description='Visualize loop closures'
    )
    
    declare_complete_reset_arg = DeclareLaunchArgument(
        'complete_reset',
        default_value='false',
        description='Complete reset'
    )
    
    declare_enable_recovery_arg = DeclareLaunchArgument(
        'enable_recovery',
        default_value='false',
        description='Enable recovery'
    )
    
    declare_synchronize_measurements_arg = DeclareLaunchArgument(
        'synchronize_measurements',
        default_value='false',
        description='Synchronize measurements'
    )
    
    declare_max_distributed_init_steps_arg = DeclareLaunchArgument(
        'max_distributed_init_steps',
        default_value='100',
        description='Maximum distributed initialization steps'
    )
    
    declare_inter_update_sleep_time_arg = DeclareLaunchArgument(
        'inter_update_sleep_time',
        default_value='5',
        description='Inter-update sleep time'
    )
    
    declare_weight_convergence_threshold_arg = DeclareLaunchArgument(
        'weight_convergence_threshold',
        default_value='1e-6',
        description='Weight convergence threshold'
    )
    
    declare_max_delayed_iterations_arg = DeclareLaunchArgument(
        'max_delayed_iterations',
        default_value='0',
        description='Maximum delayed iterations'
    )
    
    declare_timeout_threshold_arg = DeclareLaunchArgument(
        'timeout_threshold',
        default_value='15',
        description='Timeout threshold'
    )

    return LaunchDescription([
        declare_debug_arg,
        declare_agent_id_arg,
        declare_num_robots_arg,
        declare_dimension_arg,
        declare_relaxation_rank_arg,
        declare_asynchronous_arg,
        declare_asynchronous_rate_arg,
        declare_verbose_arg,
        declare_rgd_stepsize_arg,
        declare_rgd_use_preconditioner_arg,
        declare_rtr_iterations_arg,
        declare_rtr_tcg_iterations_arg,
        declare_rtr_gradnorm_tol_arg,
        declare_local_initialization_method_arg,
        declare_update_rule_arg,
        declare_multirobot_initialization_arg,
        declare_acceleration_arg,
        declare_restart_interval_arg,
        declare_robust_cost_type_arg,
        declare_gnc_use_probability_arg,
        declare_gnc_quantile_arg,
        declare_gnc_barc_arg,
        declare_gnc_mu_step_arg,
        declare_gnc_init_mu_arg,
        declare_robust_opt_num_weight_updates_arg,
        declare_robust_opt_num_resets_arg,
        declare_robust_opt_min_convergence_ratio_arg,
        declare_robust_opt_inner_iters_per_robot_arg,
        declare_robust_init_min_inliers_arg,
        declare_max_iteration_number_arg,
        declare_relative_change_tolerance_arg,
        declare_log_directory_arg,
        declare_robot_names_file_arg,
        declare_publish_iterate_arg,
        declare_visualize_loop_closures_arg,
        declare_complete_reset_arg,
        declare_enable_recovery_arg,
        declare_synchronize_measurements_arg,
        declare_max_distributed_init_steps_arg,
        declare_inter_update_sleep_time_arg,
        declare_weight_convergence_threshold_arg,
        declare_max_delayed_iterations_arg,
        declare_timeout_threshold_arg,
        OpaqueFunction(function=launch_setup),
    ])