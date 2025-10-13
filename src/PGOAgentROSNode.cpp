/* ----------------------------------------------------------------------------
 * Copyright 2020, Massachusetts Institute of Technology, * Cambridge, MA 02139
 * All Rights Reserved
 * Authors: Yulun Tian, et al. (see README for the full author list)
 * See LICENSE for the license information
 * -------------------------------------------------------------------------- */

#include <dpgo_ros/PGOAgentROS.h>

#include <cassert>
#include <map>

using namespace DPGO;

/**
This script implements the entry point for running a single PGO Agent in ROS2
*/

int main(int argc, char **argv) {
  rclcpp::init(argc, argv);

  // Create node with default name first
  auto node = rclcpp::Node::make_shared("agent");

  /**
  ###########################################
  Read unique ID of this agent
  ###########################################
  */
  int ID = -1;
  node->declare_parameter("agent_id", -1);
  ID = node->get_parameter("agent_id").as_int();
  if (ID < 0) {
    RCLCPP_ERROR(node->get_logger(), "Negative agent id!");
    return -1;
  }

  /**
  ###########################################
  Load required options
  ###########################################
  */
  int d = -1;
  int r = -1;
  int num_robots = 0;

  node->declare_parameter("num_robots", 0);
  num_robots = node->get_parameter("num_robots").as_int();
  if (num_robots <= 0) {
    RCLCPP_ERROR(node->get_logger(), "Number of robots must be positive!");
    return -1;
  }
  if (ID >= num_robots) {
    RCLCPP_ERROR(node->get_logger(), "ID greater than number of robots!");
    return -1;
  }

  node->declare_parameter("dimension", -1);
  d = node->get_parameter("dimension").as_int();
  if (d != 3) {
    RCLCPP_ERROR(node->get_logger(), "Dimension must be 3!");
    return -1;
  }

  node->declare_parameter("relaxation_rank", -1);
  r = node->get_parameter("relaxation_rank").as_int();
  if (r < d) {
    RCLCPP_ERROR(node->get_logger(),
                 "Relaxation rank cannot be smaller than dimension!");
    return -1;
  }

  dpgo_ros::PGOAgentROSParameters params(d, r, num_robots);

  /**
  ###########################################
  Load optional options
  ###########################################
  */
  // Run in asynchronous mode
  node->declare_parameter("asynchronous", false);
  params.asynchronous = node->get_parameter("asynchronous").as_bool();

  if (!params.asynchronous) {
    // Synchronous mode
    // Use Riemannian trust-region solver
    params.localOptimizationParams.method = ROptParameters::ROptMethod::RTR;
  } else {
    // Asynchronous mode
    RCLCPP_WARN(node->get_logger(), "Running asynchronous mode.");
    // Use gradient descent in asynchronous mode
    params.localOptimizationParams.method = ROptParameters::ROptMethod::RGD;
    // Frequency of optimization loop in asynchronous mode
    node->declare_parameter("asynchronous_rate", 10.0);
    params.asynchronousOptimizationRate =
        node->get_parameter("asynchronous_rate").as_double();
  }

  // Local Riemannian optimization options
  node->declare_parameter("RGD_stepsize", 1e-3);
  params.localOptimizationParams.RGD_stepsize =
      node->get_parameter("RGD_stepsize").as_double();

  node->declare_parameter("RGD_use_preconditioner", true);
  params.localOptimizationParams.RGD_use_preconditioner =
      node->get_parameter("RGD_use_preconditioner").as_bool();

  node->declare_parameter("RTR_iterations", 3);
  params.localOptimizationParams.RTR_iterations =
      static_cast<unsigned>(node->get_parameter("RTR_iterations").as_int());

  node->declare_parameter("RTR_tCG_iterations", 50);
  params.localOptimizationParams.RTR_tCG_iterations =
      static_cast<unsigned>(node->get_parameter("RTR_tCG_iterations").as_int());

  node->declare_parameter("RTR_gradnorm_tol", 1e-2);
  params.localOptimizationParams.gradnorm_tol =
      node->get_parameter("RTR_gradnorm_tol").as_double();

  // Local initialization
  std::string initMethodName = "Odometry";
  node->declare_parameter("local_initialization_method", initMethodName);
  initMethodName =
      node->get_parameter("local_initialization_method").as_string();

  if (initMethodName == "Odometry") {
    params.localInitializationMethod = InitializationMethod::Odometry;
  } else if (initMethodName == "Chordal") {
    params.localInitializationMethod = InitializationMethod::Chordal;
  } else if (initMethodName == "GNC_TLS") {
    params.localInitializationMethod = InitializationMethod::GNC_TLS;
  } else {
    RCLCPP_ERROR_STREAM(
        node->get_logger(),
        "Invalid local initialization method: " << initMethodName);
  }

  // Cross-robot initialization
  node->declare_parameter("multirobot_initialization", true);
  params.multirobotInitialization =
      node->get_parameter("multirobot_initialization").as_bool();
  if (!params.multirobotInitialization) {
    RCLCPP_WARN(node->get_logger(), "DPGO cross-robot initialization is OFF.");
  }

  // Nesterov acceleration parameters
  node->declare_parameter("acceleration", false);
  params.acceleration = node->get_parameter("acceleration").as_bool();

  int restart_interval_int = 50;
  node->declare_parameter("restart_interval", restart_interval_int);
  restart_interval_int = node->get_parameter("restart_interval").as_int();
  params.restartInterval = (unsigned)restart_interval_int;

  // Maximum delayed iterations
  node->declare_parameter("max_delayed_iterations", 0);
  params.maxDelayedIterations = static_cast<unsigned>(
      node->get_parameter("max_delayed_iterations").as_int());

  // Inter update sleep time
  node->declare_parameter("inter_update_sleep_time", 0);
  params.interUpdateSleepTime = static_cast<unsigned>(
      node->get_parameter("inter_update_sleep_time").as_int());

  // Threshold for determining measurement weight convergence
  node->declare_parameter("weight_convergence_threshold", -1.0);
  params.weightConvergenceThreshold =
      node->get_parameter("weight_convergence_threshold").as_double();

  // Timeout threshold for considering a robot disconnected
  node->declare_parameter("timeout_threshold", 15);
  params.timeoutThreshold =
      static_cast<unsigned>(node->get_parameter("timeout_threshold").as_int());

  // Stopping condition in terms of relative change
  node->declare_parameter("relative_change_tolerance", 0.1);
  params.relChangeTol =
      node->get_parameter("relative_change_tolerance").as_double();

  // Verbose flag
  node->declare_parameter("verbose", false);
  params.verbose = node->get_parameter("verbose").as_bool();

  // Publish iterate during optimization
  node->declare_parameter("publish_iterate", false);
  params.publishIterate = node->get_parameter("publish_iterate").as_bool();

  // Publish loop closures as ROS markers for visualization
  node->declare_parameter("visualize_loop_closures", false);
  params.visualizeLoopClosures =
      node->get_parameter("visualize_loop_closures").as_bool();

  // Completely reset dpgo after each distributed optimization round
  node->declare_parameter("complete_reset", false);
  params.completeReset = node->get_parameter("complete_reset").as_bool();

  // Try to recover and resume optimization after disconnection
  node->declare_parameter("enable_recovery", true);
  params.enableRecovery = node->get_parameter("enable_recovery").as_bool();

  // Synchronize shared measurements between robots before each optimization
  // round
  node->declare_parameter("synchronize_measurements", true);
  params.synchronizeMeasurements =
      node->get_parameter("synchronize_measurements").as_bool();

  // Maximum multi-robot initialization attempts
  node->declare_parameter("max_distributed_init_steps", 30);
  params.maxDistributedInitSteps = static_cast<unsigned>(
      node->get_parameter("max_distributed_init_steps").as_int());

  // Logging
  std::string log_output_path = "";
  node->declare_parameter("log_output_path", log_output_path);
  log_output_path = node->get_parameter("log_output_path").as_string();
  if (!log_output_path.empty()) {
    params.logDirectory = log_output_path;
    params.logData = true;
  } else {
    params.logData = false;
  }

  // Robust cost function
  std::string costName = "L2";
  node->declare_parameter("robust_cost_type", costName);
  costName = node->get_parameter("robust_cost_type").as_string();

  if (costName == "L2") {
    params.robustCostParams.costType = RobustCostParameters::Type::L2;
  } else if (costName == "L1") {
    params.robustCostParams.costType = RobustCostParameters::Type::L1;
  } else if (costName == "Huber") {
    params.robustCostParams.costType = RobustCostParameters::Type::Huber;
  } else if (costName == "TLS") {
    params.robustCostParams.costType = RobustCostParameters::Type::TLS;
  } else if (costName == "GM") {
    params.robustCostParams.costType = RobustCostParameters::Type::GM;
  } else if (costName == "GNC_TLS") {
    params.robustCostParams.costType = RobustCostParameters::Type::GNC_TLS;
  } else {
    RCLCPP_ERROR_STREAM(node->get_logger(),
                        "Unknown robust cost type: " << costName);
    rclcpp::shutdown();
    return -1;
  }

  // GNC parameters
  bool gnc_use_quantile = false;
  node->declare_parameter("GNC_use_probability", gnc_use_quantile);
  gnc_use_quantile = node->get_parameter("GNC_use_probability").as_bool();

  if (gnc_use_quantile) {
    double gnc_quantile = 0.9;
    node->declare_parameter("GNC_quantile", gnc_quantile);
    gnc_quantile = node->get_parameter("GNC_quantile").as_double();
    double gnc_barc =
        RobustCost::computeErrorThresholdAtQuantile(gnc_quantile, 3);
    params.robustCostParams.GNCBarc = gnc_barc;
    RCLCPP_INFO(node->get_logger(),
                "PGOAgentROS: set GNC confidence quantile at %f (barc %f).",
                gnc_quantile, gnc_barc);
  } else {
    double gnc_barc = 5.0;
    node->declare_parameter("GNC_barc", gnc_barc);
    gnc_barc = node->get_parameter("GNC_barc").as_double();
    params.robustCostParams.GNCBarc = gnc_barc;
    RCLCPP_INFO(node->get_logger(), "PGOAgentROS: set GNC barc at %f.",
                gnc_barc);
  }

  node->declare_parameter("GNC_mu_step", 2.0);
  params.robustCostParams.GNCMuStep =
      node->get_parameter("GNC_mu_step").as_double();

  node->declare_parameter("GNC_init_mu", 1e-5);
  params.robustCostParams.GNCInitMu =
      node->get_parameter("GNC_init_mu").as_double();

  node->declare_parameter("robust_opt_num_weight_updates", 4);
  params.robustOptNumWeightUpdates = static_cast<unsigned>(
      node->get_parameter("robust_opt_num_weight_updates").as_int());

  node->declare_parameter("robust_opt_num_resets", 0);
  params.robustOptNumResets = static_cast<unsigned>(
      node->get_parameter("robust_opt_num_resets").as_int());

  node->declare_parameter("robust_opt_min_convergence_ratio", 0.0);
  params.robustOptMinConvergenceRatio =
      node->get_parameter("robust_opt_min_convergence_ratio").as_double();

  int robust_opt_inner_iters_per_robot = 10;
  node->declare_parameter("robust_opt_inner_iters_per_robot",
                          robust_opt_inner_iters_per_robot);
  robust_opt_inner_iters_per_robot =
      node->get_parameter("robust_opt_inner_iters_per_robot").as_int();
  params.robustOptInnerIters = num_robots * robust_opt_inner_iters_per_robot;

  int robust_init_min_inliers = 5;
  node->declare_parameter("robust_init_min_inliers", robust_init_min_inliers);
  robust_init_min_inliers =
      node->get_parameter("robust_init_min_inliers").as_int();
  params.robustInitMinInliers = static_cast<unsigned>(robust_init_min_inliers);

  // Maximum number of iterations
  int max_iters_int = 1000;
  node->declare_parameter("max_iteration_number", max_iters_int);
  max_iters_int = node->get_parameter("max_iteration_number").as_int();
  params.maxNumIters = static_cast<unsigned>(max_iters_int);
  // For robust optimization, we set the number of iterations based on the
  // number of GNC iterations
  if (costName != "L2") {
    max_iters_int =
        (params.robustOptNumWeightUpdates + 1) * params.robustOptInnerIters - 2;
    max_iters_int = std::max(max_iters_int, 0);
    params.maxNumIters = (unsigned)max_iters_int;
  }

  // Update rule
  std::string update_rule_str = "Uniform";
  node->declare_parameter("update_rule", update_rule_str);
  update_rule_str = node->get_parameter("update_rule").as_string();

  if (update_rule_str == "Uniform") {
    params.updateRule = dpgo_ros::PGOAgentROSParameters::UpdateRule::Uniform;
  } else if (update_rule_str == "RoundRobin") {
    params.updateRule = dpgo_ros::PGOAgentROSParameters::UpdateRule::RoundRobin;
  } else {
    RCLCPP_ERROR_STREAM(node->get_logger(),
                        "Unknown update rule: " << update_rule_str);
    rclcpp::shutdown();
    return -1;
  }

  // Print params
  RCLCPP_INFO_STREAM(node->get_logger(), "Initializing PGOAgent "
                                             << ID << " with params: \n"
                                             << params);

  /**
  ###########################################
  Initialize PGO agent
  ###########################################
  */
  dpgo_ros::PGOAgentROS agent(node, ID, params);

  // // Use a MultiThreadedExecutor so callbacks can run concurrently if needed.
  // auto exec = std::make_shared<rclcpp::executors::MultiThreadedExecutor>(
  //     rclcpp::ExecutorOptions(), /*num_threads=*/2);
  // exec->add_node(node);

  // exec->spin();

  rclcpp::Rate rate(100);
  while (rclcpp::ok()) {
    rclcpp::spin_some(node);
    agent.runOnce();
    rate.sleep();
  }
  rclcpp::shutdown();
  return 0;
}