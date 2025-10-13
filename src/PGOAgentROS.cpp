/* ----------------------------------------------------------------------------
 * Copyright 2020, Massachusetts Institute of Technology, * Cambridge, MA 02139
 * All Rights Reserved
 * Authors: Yulun Tian, et al. (see README for the full author list)
 * See LICENSE for the license information
 * -------------------------------------------------------------------------- */

#include <chrono>
#include <dpgo_ros/PGOAgentROS.h>
#include <dpgo_ros/utils.h>
#include <functional>
#include <geometry_msgs/msg/pose_array.hpp>
#include <glog/logging.h>
#include <map>
#include <nav_msgs/msg/path.hpp>
#include <pose_graph_tools_msgs/srv/pose_graph_query.hpp>
#include <pose_graph_tools_ros/utils.h>
#include <random>
#include <std_msgs/msg/int32_multi_array.hpp>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <thread>

// Include DPGO after other headers to avoid PI constant conflicts
#include <DPGO/DPGO_solver.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <thread>
#include <visualization_msgs/msg/marker.hpp>

using namespace DPGO;
using dpgo_ros::msg::Command;
using dpgo_ros::msg::MatrixMsg;
using dpgo_ros::msg::PublicPoses;
using dpgo_ros::msg::RelativeMeasurementList;
using dpgo_ros::msg::RelativeMeasurementWeights;
using dpgo_ros::msg::Status;

namespace dpgo_ros {

PGOAgentROS::PGOAgentROS(std::shared_ptr<rclcpp::Node> node, unsigned ID,
                         const PGOAgentROSParameters &params)
    : PGOAgent(ID, params), node_(node), mParamsROS(params), mClusterID(ID),
      mInitStepsDone(0), mTotalBytesReceived(0), mIterationElapsedMs(0) {
  mTeamIterRequired.assign(mParams.numRobots, 0);
  mTeamIterReceived.assign(mParams.numRobots, 0);
  mTeamReceivedSharedLoopClosures.assign(mParams.numRobots, false);
  mTeamConnected.assign(mParams.numRobots, true);

  // Load robot names
  for (size_t id = 0; id < mParams.numRobots; id++) {
    std::string robot_name = "kimera" + std::to_string(id);
    node_->declare_parameter("robot" + std::to_string(id) + "_name",
                             robot_name);
    node_->get_parameter("robot" + std::to_string(id) + "_name", robot_name);
    mRobotNames[id] = robot_name;
  }

  // ROS subscriber
  for (size_t robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    std::string topic_prefix =
        "/" + mRobotNames.at(robot_id) + "/dpgo_ros_node/";
    mLiftingMatrixSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::MatrixMsg>(
            topic_prefix + "lifting_matrix", 100,
            std::bind(&PGOAgentROS::liftingMatrixCallback, this,
                      std::placeholders::_1)));
    mStatusSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::Status>(
            topic_prefix + "status", 100,
            std::bind(&PGOAgentROS::statusCallback, this,
                      std::placeholders::_1)));
    mCommandSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::Command>(
            topic_prefix + "command", 100,
            std::bind(&PGOAgentROS::commandCallback, this,
                      std::placeholders::_1)));
    mAnchorSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::PublicPoses>(
            topic_prefix + "anchor", 100,
            std::bind(&PGOAgentROS::anchorCallback, this,
                      std::placeholders::_1)));
    mPublicPosesSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::PublicPoses>(
            topic_prefix + "public_poses", 100,
            std::bind(&PGOAgentROS::publicPosesCallback, this,
                      std::placeholders::_1)));
    mSharedLoopClosureSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::RelativeMeasurementList>(
            topic_prefix + "public_measurements", 100,
            std::bind(&PGOAgentROS::publicMeasurementsCallback, this,
                      std::placeholders::_1)));
  }
  mConnectivitySubscriber =
      node_->create_subscription<std_msgs::msg::UInt16MultiArray>(
          "/" + mRobotNames.at(mID) + "/connected_peer_ids", 5,
          std::bind(&PGOAgentROS::connectivityCallback, this,
                    std::placeholders::_1));

  for (size_t robot_id = 0; robot_id < getID(); ++robot_id) {
    std::string topic_prefix =
        "/" + mRobotNames.at(robot_id) + "/dpgo_ros_node/";
    mMeasurementWeightsSubscriber.push_back(
        node_->create_subscription<dpgo_ros::msg::RelativeMeasurementWeights>(
            topic_prefix + "measurement_weights", 100,
            std::bind(&PGOAgentROS::measurementWeightsCallback, this,
                      std::placeholders::_1)));
  }

  // ROS publisher
  mLiftingMatrixPublisher =
      node_->create_publisher<dpgo_ros::msg::MatrixMsg>("lifting_matrix", 1);
  mAnchorPublisher =
      node_->create_publisher<dpgo_ros::msg::PublicPoses>("anchor", 1);
  mStatusPublisher =
      node_->create_publisher<dpgo_ros::msg::Status>("status", 1);
  mCommandPublisher =
      node_->create_publisher<dpgo_ros::msg::Command>("command", 20);
  mPublicPosesPublisher =
      node_->create_publisher<dpgo_ros::msg::PublicPoses>("public_poses", 20);
  mPublicMeasurementsPublisher =
      node_->create_publisher<dpgo_ros::msg::RelativeMeasurementList>(
          "public_measurements", 20);
  mMeasurementWeightsPublisher =
      node_->create_publisher<dpgo_ros::msg::RelativeMeasurementWeights>(
          "measurement_weights", 20);
  mPoseArrayPublisher =
      node_->create_publisher<geometry_msgs::msg::PoseArray>("trajectory", 1);
  mPathPublisher = node_->create_publisher<nav_msgs::msg::Path>("path", 1);
  mPoseGraphPublisher =
      node_->create_publisher<pose_graph_tools_msgs::msg::PoseGraph>(
          "optimized_pose_graph", 1);
  mLoopClosureMarkerPublisher =
      node_->create_publisher<visualization_msgs::msg::Marker>("loop_closures",
                                                               1);

  srv_callback_group_ =
      node_->create_callback_group(rclcpp::CallbackGroupType::Reentrant);

  auto service_qos =
      rclcpp::QoS(rclcpp::KeepLast(10)).reliable().durability_volatile();
  mPoseGraphClient =
      node_->create_client<pose_graph_tools_msgs::srv::PoseGraphQuery>(
          "/" + mRobotNames.at(getID()) +
              "/distributed_loop_closure/request_pose_graph",
          service_qos, srv_callback_group_);

  // ROS timer
  timer = node_->create_wall_timer(
      std::chrono::seconds(3), std::bind(&PGOAgentROS::timerCallback, this));
  mVisualizationTimer = node_->create_wall_timer(
      std::chrono::seconds(30),
      std::bind(&PGOAgentROS::visualizationTimerCallback, this));

  // Initially, assume each robot is in a separate cluster
  resetRobotClusterIDs();

  // Publish lifting matrix
  for (size_t iter_ = 0; iter_ < 10; ++iter_) {
    publishNoopCommand();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  mLastResetTime = node_->now();
  mLaunchTime = node_->now();
  mLastCommandTime = node_->now();
  mLastUpdateTime.reset();
}

void PGOAgentROS::runOnce() {
  if (mParams.asynchronous) {
    runOnceAsynchronous();
  } else {
    runOnceSynchronous();
  }

  if (mPublishPublicPosesRequested) {
    publishPublicPoses(false);
    if (mParams.acceleration)
      publishPublicPoses(true);
    mPublishPublicPosesRequested = false;
  }

  checkTimeout();
  // checkDisconnectedRobot();
}

void PGOAgentROS::runOnceAsynchronous() {
  if (mPublishAsynchronousRequested) {
    if (isLeader())
      publishAnchor();
    publishStatus();
    publishIterate();
    logIteration();
    mPublishAsynchronousRequested = false;
  }
}

void PGOAgentROS::runOnceSynchronous() {
  CHECK(!mParams.asynchronous);

  // Perform an optimization step
  if (mSynchronousOptimizationRequested) {

    // Check if ready to perform iterate
    bool ready = true;
    for (unsigned neighbor : mPoseGraph->activeNeighborIDs()) {
      int requiredIter = (int)mTeamIterRequired[neighbor];
      if (mParams.acceleration)
        requiredIter = (int)iteration_number() + 1;
      requiredIter = requiredIter - mParamsROS.maxDelayedIterations;
      if ((int)mTeamIterReceived[neighbor] < requiredIter) {
        ready = false;
        static auto last_warn_time = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now -
                                                             last_warn_time)
                .count() >= 1) {
          RCLCPP_WARN(node_->get_logger(),
                      "Robot %u iteration %u waits for neighbor %u "
                      "iteration %u (last received %u).",
                      getID(), iteration_number() + 1, neighbor, requiredIter,
                      mTeamIterReceived[neighbor]);
          last_warn_time = now;
        }
      }
    }

    // Perform iterate with optimization if ready
    if (ready) {
      // Beta feature: Apply stored neighbor poses and edge weights for inactive
      // robots setInactiveNeighborPoses(); setInactiveEdgeWeights();
      // mPoseGraph->useInactiveNeighbors(true);

      // Iterate
      auto startTime = std::chrono::high_resolution_clock::now();
      bool success = iterate(true);
      auto counter = std::chrono::high_resolution_clock::now() - startTime;
      mIterationElapsedMs =
          (double)std::chrono::duration_cast<std::chrono::milliseconds>(counter)
              .count();
      mSynchronousOptimizationRequested = false;
      if (success) {
        mLastUpdateTime.emplace(node_->now());
        RCLCPP_INFO(node_->get_logger(),
                    "Robot %u iteration %u: success=%d, func_decr=%.1e, "
                    "grad_init=%.1e, grad_opt=%.1e.",
                    getID(), iteration_number(), mLocalOptResult.success,
                    mLocalOptResult.fInit - mLocalOptResult.fOpt,
                    mLocalOptResult.gradNormInit, mLocalOptResult.gradNormOpt);
      } else {
        RCLCPP_WARN(node_->get_logger(), "Robot %u iteration not successful!",
                    getID());
      }

      // First robot publish anchor
      if (isLeader()) {
        publishAnchor();
      }

      // Publish status
      publishStatus();

      // Publish iterate (for visualization)
      publishIterate();

      // Log local iteration
      logIteration();

      // Print information
      if (isLeader() && mParams.verbose) {
        RCLCPP_INFO(node_->get_logger(),
                    "Num weight updates done: %i, num inner iters: %i.",
                    mWeightUpdateCount, mRobustOptInnerIter);
        for (size_t robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
          if (!isRobotActive(robot_id))
            continue;
          const auto &it = mTeamStatus.find(robot_id);
          if (it != mTeamStatus.end()) {
            const auto &robot_status = it->second;
            RCLCPP_INFO(node_->get_logger(), "Robot %zu relative change %f.",
                        robot_id, robot_status.relativeChange);
          } else {
            RCLCPP_INFO(node_->get_logger(), "Robot %zu status unavailable.",
                        robot_id);
          }
        }
      }

      // Check termination condition OR notify next robot to update
      if (isLeader()) {
        if (shouldTerminate()) {
          publishTerminateCommand();
        } else if (shouldUpdateMeasurementWeights()) {
          publishUpdateWeightCommand();
        } else {
          publishUpdateCommand();
        }
      } else {
        publishUpdateCommand();
      }
    }
  }
}

void PGOAgentROS::reset() {
  PGOAgent::reset();
  mSynchronousOptimizationRequested = false;
  mTryInitializeRequested = false;
  mInitStepsDone = 0;
  mTeamIterRequired.assign(mParams.numRobots, 0);
  mTeamIterReceived.assign(mParams.numRobots, 0);
  mTeamReceivedSharedLoopClosures.assign(mParams.numRobots, false);
  mTotalBytesReceived = 0;
  mTeamStatusMsg.clear();
  if (mIterationLog.is_open()) {
    mIterationLog.close();
  }
  if (mParamsROS.completeReset) {
    RCLCPP_WARN(node_->get_logger(), "Reset DPGO completely.");
    mPoseGraph = std::make_shared<PoseGraph>(mID, r, d); // Reset pose graph
    mCachedPoses.reset(); // Reset stored trajectory estimate
    mCachedLoopClosureMarkers.reset();
  }
  resetRobotClusterIDs();
  mLastResetTime = node_->now();
  mLastUpdateTime.reset();
}

bool PGOAgentROS::requestPoseGraph() {
  // Query local pose graph
  auto request =
      std::make_shared<pose_graph_tools_msgs::srv::PoseGraphQuery::Request>();
  request->robot_id = getID();
  // Wait for service to be available
  if (!mPoseGraphClient->wait_for_service(std::chrono::seconds(5))) {
    RCLCPP_ERROR_STREAM(node_->get_logger(),
                        "ROS service " << mPoseGraphClient->get_service_name()
                                       << " does not exist!");
    return false;
  }

  // Call service synchronously
  auto future = mPoseGraphClient->async_send_request(request);

  // Wait for the future without spinning the node (since it's already being
  // spun in main)
  auto status = future.wait_for(std::chrono::seconds(10));
  if (status != std::future_status::ready) {
    RCLCPP_ERROR_STREAM(node_->get_logger(),
                        "Service call to "
                            << mPoseGraphClient->get_service_name()
                            << " timed out");
    return false;
  }

  // auto status = rclcpp::spin_until_future_complete(node_, future);
  // if (status != rclcpp::FutureReturnCode::SUCCESS) {
  //   RCLCPP_ERROR_STREAM(node_->get_logger(),
  //                       "Failed to call ROS service " <<
  //                       mPoseGraphClient->get_service_name());
  //   return false;
  // }

  auto response = future.get();
  pose_graph_tools_msgs::msg::PoseGraph pose_graph = response->pose_graph;
  if (pose_graph.edges.size() <= 1) {
    RCLCPP_WARN(node_->get_logger(), "Received empty pose graph.");
    return false;
  }

  // Process edges
  unsigned int num_measurements_before = mPoseGraph->numMeasurements();
  for (const auto &edge : pose_graph.edges) {
    RelativeSEMeasurement m = RelativeMeasurementFromMsg(edge);
    const PoseID src_id(m.r1, m.p1);
    const PoseID dst_id(m.r2, m.p2);
    if (m.r1 != getID() && m.r2 != getID()) {
      RCLCPP_ERROR(node_->get_logger(),
                   "Robot %u received irrelevant measurement! ", getID());
    }
    if (!mPoseGraph->hasMeasurement(src_id, dst_id)) {
      addMeasurement(m);
    }
  }
  unsigned int num_measurements_after = mPoseGraph->numMeasurements();
  RCLCPP_INFO(node_->get_logger(),
              "Received pose graph from ROS service (%u new measurements).",
              num_measurements_after - num_measurements_before);

  // Process nodes
  PoseArray initial_poses(dimension(), num_poses());
  if (!pose_graph.nodes.empty()) {
    // Filter nodes that do not belong to this robot
    vector<pose_graph_tools_msgs::msg::PoseGraphNode> nodes_filtered;
    for (const auto &node : pose_graph.nodes) {
      if ((unsigned)node.robot_id == getID())
        nodes_filtered.push_back(node);
    }
    // If pose graph contains initial guess for the poses, we will use them
    size_t num_nodes = nodes_filtered.size();
    if (num_nodes == num_poses()) {
      for (const auto &node : nodes_filtered) {
        assert((unsigned)node.robot_id == getID());
        size_t index = node.key;
        assert(index >= 0 && index < num_poses());
        initial_poses.rotation(index) = RotationFromPoseMsg(node.pose);
        initial_poses.translation(index) = TranslationFromPoseMsg(node.pose);
      }
    }
  }
  if (mParamsROS.synchronizeMeasurements) {
    // Synchronize shared measurements with other robots
    mTeamReceivedSharedLoopClosures.assign(mParams.numRobots, false);
    mTeamReceivedSharedLoopClosures[getID()] = true;

    // In Kimera-Multi, we wait for inter-robot loops
    // from robots with smaller ID
    // for (size_t robot_id = getID(); robot_id < mParams.numRobots; ++robot_id)
    //   mTeamReceivedSharedLoopClosures[robot_id] = true;
  } else {
    // Assume measurements are already synchronized by front end
    mTeamReceivedSharedLoopClosures.assign(mParams.numRobots, true);
  }

  mTryInitializeRequested = true;
  return true;
}

bool PGOAgentROS::tryInitialize() {
  // Before initialization, we need to received inter-robot loop closures from
  // all preceeding robots.
  bool ready = true;
  for (unsigned robot_id = 0; robot_id < getID(); ++robot_id) {
    // Skip if this preceeding robot is excluded from optimization
    if (!isRobotActive(robot_id)) {
      continue;
    }
    if (!mTeamReceivedSharedLoopClosures[robot_id]) {
      RCLCPP_INFO(node_->get_logger(),
                  "Robot %u waiting for shared loop closures from robot %u.",
                  getID(), robot_id);
      ready = false;
      break;
    }
  }
  if (ready) {
    RCLCPP_INFO(node_->get_logger(),
                "Robot %u initializes. "
                "num_poses:%u, odom:%u, local_lc:%u, shared_lc:%u.",
                getID(), num_poses(), mPoseGraph->numOdometry(),
                mPoseGraph->numPrivateLoopClosures(),
                mPoseGraph->numSharedLoopClosures());

    // Perform local initialization
    initialize();

    // Leader first initializes in global frame
    if (isLeader()) {
      if (getID() == 0) {
        initializeInGlobalFrame(Pose(d));
      } else if (getID() != 0 && mCachedPoses.has_value()) {
        RCLCPP_INFO(
            node_->get_logger(),
            "Leader %u initializes in global frame using previous result.",
            getID());
        const auto TPrev = mCachedPoses.value();
        const Pose T_world_leader(TPrev.pose(0));
        initializeInGlobalFrame(T_world_leader);
        initializeGlobalAnchor();
        anchorFirstPose();
      }
    }
    mTryInitializeRequested = false;
  }
  return ready;
}

bool PGOAgentROS::isRobotConnected(unsigned robot_id) const {
  if (robot_id >= mParams.numRobots) {
    return false;
  }
  if (robot_id == getID()) {
    return true;
  }
  return mTeamConnected[robot_id];
}

void PGOAgentROS::setActiveRobots() {
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (isRobotConnected(robot_id) && getRobotClusterID(robot_id) == getID()) {
      RCLCPP_INFO(node_->get_logger(), "Set robot %u to active.", robot_id);
      setRobotActive(robot_id, true);
    } else {
      RCLCPP_WARN(node_->get_logger(), "Set robot %u to inactive.", robot_id);
      RCLCPP_WARN(node_->get_logger(),
                  "Robot %u cluster ID %d, self cluster ID %d.", robot_id,
                  getRobotClusterID(robot_id), getID());
      setRobotActive(robot_id, false);
    }
  }
}

void PGOAgentROS::updateActiveRobots(
    const dpgo_ros::msg::Command::SharedPtr msg) {
  std::set<unsigned> active_robots_set(msg->active_robots.begin(),
                                       msg->active_robots.end());
  for (unsigned int robot_id = 0; robot_id < mParams.numRobots; robot_id++) {
    if (active_robots_set.find(robot_id) == active_robots_set.end()) {
      setRobotActive(robot_id, false);
    } else {
      setRobotActive(robot_id, true);
    }
  }
}

void PGOAgentROS::publishLiftingMatrix() {
  Matrix YLift;
  if (!getLiftingMatrix(YLift)) {
    RCLCPP_WARN(node_->get_logger(), "Lifting matrix does not exist! ");
    return;
  }
  dpgo_ros::msg::MatrixMsg msg = MatrixToMsg(YLift);
  mLiftingMatrixPublisher->publish(msg);
}

void PGOAgentROS::publishAnchor() {
  // We assume the anchor is always the first pose of the first robot
  if (!isLeader()) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Only leader robot should publish anchor!");
    return;
  }
  if (mState != PGOAgentState::INITIALIZED) {
    RCLCPP_WARN(node_->get_logger(), "Cannot publish anchor: not initialized.");
    return;
  }
  Matrix T0;
  if (getID() == 0) {
    getSharedPose(0, T0);
  } else {
    if (!globalAnchor.has_value()) {
      return;
    }
    T0 = globalAnchor.value().getData();
  }
  dpgo_ros::msg::PublicPoses msg;
  msg.robot_id = 0;
  msg.instance_number = instance_number();
  msg.iteration_number = iteration_number();
  msg.cluster_id = getClusterID();
  msg.is_auxiliary = false;
  msg.pose_ids.push_back(0);
  msg.poses.push_back(MatrixToMsg(T0));

  mAnchorPublisher->publish(msg);
}

void PGOAgentROS::publishUpdateCommand() {
  unsigned selected_robot = 0;
  switch (mParamsROS.updateRule) {
  case PGOAgentROSParameters::UpdateRule::Uniform: {
    // Uniform sampling of all active robots
    std::vector<unsigned> active_robots;
    for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
      if (isRobotActive(robot_id) && isRobotInitialized(robot_id)) {
        active_robots.push_back(robot_id);
      }
    }
    size_t num_active_robots = active_robots.size();
    std::vector<double> weights(num_active_robots, 1.0);
    std::discrete_distribution<int> distribution(weights.begin(),
                                                 weights.end());
    std::random_device rd;
    std::mt19937 gen(rd());
    selected_robot = active_robots[distribution(gen)];
    break;
  }
  case PGOAgentROSParameters::UpdateRule::RoundRobin: {
    // Round robin updates
    unsigned next_robot_id = (getID() + 1) % mParams.numRobots;
    while (!isRobotActive(next_robot_id) ||
           !isRobotInitialized(next_robot_id)) {
      next_robot_id = (next_robot_id + 1) % mParams.numRobots;
    }
    selected_robot = next_robot_id;
    break;
  }
  }
  if (selected_robot == getID()) {
    RCLCPP_WARN(node_->get_logger(),
                "[publishUpdateCommand] Robot %u selects self to update next!",
                getID());
  }
  publishUpdateCommand(selected_robot);
}

void PGOAgentROS::publishUpdateCommand(unsigned robot_id) {
  if (mParams.asynchronous) {
    // In asynchronous mode, no need to publish update command
    // because each robot's local optimziation loop is constantly running
    return;
  }
  if (!isRobotActive(robot_id)) {
    RCLCPP_ERROR(node_->get_logger(), "Next robot to update %u is not active!",
                 robot_id);
    return;
  }
  if (mParamsROS.interUpdateSleepTime > 1e-3)
    std::this_thread::sleep_for(
        std::chrono::duration<double>(mParamsROS.interUpdateSleepTime));
  dpgo_ros::msg::Command msg;
  msg.header.stamp = node_->now();
  msg.command = dpgo_ros::msg::Command::UPDATE;
  msg.cluster_id = getClusterID();
  msg.publishing_robot = getID();
  msg.executing_robot = robot_id;
  msg.executing_iteration = iteration_number() + 1;
  RCLCPP_INFO_STREAM(node_->get_logger(),
                     "Send UPDATE to robot " << msg.executing_robot
                                             << " to perform iteration "
                                             << msg.executing_iteration << ".");
  mCommandPublisher->publish(msg);
}

void PGOAgentROS::publishRecoverCommand() {
  dpgo_ros::msg::Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::RECOVER;
  msg.executing_iteration = iteration_number();
  mCommandPublisher->publish(msg);
  RCLCPP_INFO(node_->get_logger(), "Robot %u published RECOVER command.",
              getID());
}

void PGOAgentROS::publishTerminateCommand() {
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::TERMINATE;
  mCommandPublisher->publish(msg);
  RCLCPP_INFO(node_->get_logger(), "Robot %u published TERMINATE command.",
              getID());
}

void PGOAgentROS::publishHardTerminateCommand() {
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::HARD_TERMINATE;
  mCommandPublisher->publish(msg);
  RCLCPP_INFO(node_->get_logger(), "Robot %u published HARD_TERMINATE command.",
              getID());
}

void PGOAgentROS::publishUpdateWeightCommand() {
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::UPDATE_WEIGHT;
  mCommandPublisher->publish(msg);
  RCLCPP_INFO(node_->get_logger(),
              "Robot %u published UPDATE_WEIGHT command (num inner iters %i).",
              getID(), mRobustOptInnerIter);
}

void PGOAgentROS::publishRequestPoseGraphCommand() {
  if (!isLeader()) {
    RCLCPP_WARN(node_->get_logger(),
                "Only leader should send request pose graph command! ");
    return;
  }
  setActiveRobots();
  if (numActiveRobots() == 1) {
    RCLCPP_WARN(
        node_->get_logger(),
        "Not enough active robots. Do not publish request pose graph command.");
    return;
  }
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::REQUEST_POSE_GRAPH;
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (isRobotActive(robot_id)) {
      msg.active_robots.push_back(robot_id);
    }
  }
  mCommandPublisher->publish(msg);
  RCLCPP_INFO(node_->get_logger(),
              "Robot %u published REQUEST_POSE_GRAPH command.", getID());
}

void PGOAgentROS::publishInitializeCommand() {
  if (!isLeader()) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Only leader should send INITIALIZE command!");
  }
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::INITIALIZE;
  mCommandPublisher->publish(msg);
  mInitStepsDone++;
  mPublishInitializeCommandRequested = false;
  RCLCPP_INFO(node_->get_logger(), "Robot %u published INITIALIZE command.",
              getID());
}

void PGOAgentROS::publishActiveRobotsCommand() {
  if (!isLeader()) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Only leader should publish active robots!");
    return;
  }
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::SET_ACTIVE_ROBOTS;
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (isRobotActive(robot_id)) {
      msg.active_robots.push_back(robot_id);
    }
  }

  mCommandPublisher->publish(msg);
}

void PGOAgentROS::publishNoopCommand() {
  Command msg;
  msg.header.stamp = rclcpp::Time(node_->now());
  msg.publishing_robot = getID();
  msg.cluster_id = getClusterID();
  msg.command = Command::NOOP;
  mCommandPublisher->publish(msg);
}

void PGOAgentROS::publishStatus() {
  Status msg = statusToMsg(getStatus());
  msg.cluster_id = getClusterID();
  msg.header.stamp = rclcpp::Time(node_->now());
  mStatusPublisher->publish(msg);
}

void PGOAgentROS::storeOptimizedTrajectory() {
  PoseArray T(dimension(), num_poses());
  if (getTrajectoryInGlobalFrame(T)) {
    mCachedPoses.emplace(T);
  }
}

void PGOAgentROS::publishTrajectory(const PoseArray &T) {
  // Publish as pose array
  geometry_msgs::msg::PoseArray pose_array =
      TrajectoryToPoseArray(T.d(), T.n(), T.getData());
  mPoseArrayPublisher->publish(pose_array);

  // Publish as path
  nav_msgs::msg::Path path = TrajectoryToPath(T.d(), T.n(), T.getData());
  mPathPublisher->publish(path);

  // Publish as optimized pose graph
  pose_graph_tools_msgs::msg::PoseGraph pose_graph =
      TrajectoryToPoseGraphMsg(getID(), T.d(), T.n(), T.getData());
  mPoseGraphPublisher->publish(pose_graph);
}

void PGOAgentROS::publishOptimizedTrajectory() {
  if (!isRobotActive(getID()))
    return;
  if (!mCachedPoses.has_value())
    return;
  publishTrajectory(mCachedPoses.value());
}

void PGOAgentROS::publishIterate() {
  if (!mParamsROS.publishIterate) {
    return;
  }
  PoseArray T(dimension(), num_poses());
  if (getTrajectoryInGlobalFrame(T)) {
    publishTrajectory(T);
  }
}

void PGOAgentROS::publishPublicPoses(bool aux) {
  for (unsigned neighbor : getNeighbors()) {
    PoseDict map;
    if (aux) {
      if (!getAuxSharedPoseDictWithNeighbor(map, neighbor))
        return;
    } else {
      if (!getSharedPoseDictWithNeighbor(map, neighbor))
        return;
    }
    if (map.empty())
      continue;

    PublicPoses msg;
    msg.robot_id = getID();
    msg.cluster_id = getClusterID();
    msg.destination_robot_id = neighbor;
    msg.instance_number = instance_number();
    msg.iteration_number = iteration_number();
    msg.is_auxiliary = aux;

    for (const auto &sharedPose : map) {
      const PoseID nID = sharedPose.first;
      const auto &matrix = sharedPose.second.getData();
      CHECK_EQ(nID.robot_id, getID());
      msg.pose_ids.push_back(nID.frame_id);
      msg.poses.push_back(MatrixToMsg(matrix));
    }
    mPublicPosesPublisher->publish(msg);
  }
}

void PGOAgentROS::publishPublicMeasurements() {
  if (!mParamsROS.synchronizeMeasurements) {
    // Do not publish shared measurements
    // when assuming measurements are already synched
    return;
  }
  std::map<unsigned, RelativeMeasurementList> msg_map;
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    RelativeMeasurementList msg;
    msg.from_robot = getID();
    msg.from_cluster = getClusterID();
    msg.to_robot = robot_id;
    msg_map[robot_id] = msg;
  }
  for (const auto &m : mPoseGraph->sharedLoopClosures()) {
    unsigned otherID = 0;
    if (m.r1 == getID()) {
      otherID = m.r2;
    } else {
      otherID = m.r1;
    }
    CHECK(msg_map.find(otherID) != msg_map.end());
    const auto edge = RelativeMeasurementToMsg(m);
    msg_map[otherID].edges.push_back(edge);
  }
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id)
    mPublicMeasurementsPublisher->publish(msg_map[robot_id]);
}

void PGOAgentROS::publishMeasurementWeights() {
  // if (mState != PGOAgentState::INITIALIZED) return;

  std::map<unsigned, RelativeMeasurementWeights> msg_map;
  for (const auto &m : mPoseGraph->sharedLoopClosures()) {
    unsigned otherID = 0;
    if (m.r1 == getID()) {
      otherID = m.r2;
    } else {
      otherID = m.r1;
    }
    if (otherID > getID()) {
      if (msg_map.find(otherID) == msg_map.end()) {
        RelativeMeasurementWeights msg;
        msg.robot_id = getID();
        msg.cluster_id = getClusterID();
        msg.destination_robot_id = otherID;
        msg_map[otherID] = msg;
      }
      msg_map[otherID].src_robot_ids.push_back(m.r1);
      msg_map[otherID].dst_robot_ids.push_back(m.r2);
      msg_map[otherID].src_pose_ids.push_back(m.p1);
      msg_map[otherID].dst_pose_ids.push_back(m.p2);
      msg_map[otherID].weights.push_back(m.weight);
      msg_map[otherID].fixed_weights.push_back(m.fixedWeight);
    }
  }
  for (const auto &it : msg_map) {
    const auto &msg = it.second;
    if (!msg.weights.empty()) {
      mMeasurementWeightsPublisher->publish(msg);
    }
  }
}

void PGOAgentROS::storeLoopClosureMarkers() {
  if (mState != PGOAgentState::INITIALIZED)
    return;
  double weight_tol = mParamsROS.weightConvergenceThreshold;
  visualization_msgs::msg::Marker line_list;
  line_list.id = (int)getID();
  line_list.type = visualization_msgs::msg::Marker::LINE_LIST;
  line_list.scale.x = 0.1;
  line_list.header.frame_id = "/world";
  line_list.color.a = 1.0;
  line_list.pose.orientation.x = 0.0;
  line_list.pose.orientation.y = 0.0;
  line_list.pose.orientation.z = 0.0;
  line_list.pose.orientation.w = 1.0;
  line_list.action = visualization_msgs::msg::Marker::ADD;
  for (const auto &measurement : mPoseGraph->privateLoopClosures()) {
    Matrix T1, T2, t1, t2;
    bool b1, b2;
    geometry_msgs::msg::Point p1, p2;
    b1 = getPoseInGlobalFrame(measurement.p1, T1);
    b2 = getPoseInGlobalFrame(measurement.p2, T2);
    if (b1 && b2) {
      t1 = T1.block(0, d, d, 1);
      t2 = T2.block(0, d, d, 1);
      p1.x = t1(0);
      p1.y = t1(1);
      p1.z = t1(2);
      p2.x = t2(0);
      p2.y = t2(1);
      p2.z = t2(2);
      line_list.points.push_back(p1);
      line_list.points.push_back(p2);
      std_msgs::msg::ColorRGBA line_color;
      line_color.a = 1;
      if (measurement.weight > 1 - weight_tol) {
        line_color.g = 1;
      } else if (measurement.weight < weight_tol) {
        line_color.r = 1;
      } else {
        line_color.b = 1;
      }
      line_list.colors.push_back(line_color);
      line_list.colors.push_back(line_color);
    }
  }
  for (const auto &measurement : mPoseGraph->sharedLoopClosures()) {
    Matrix mT, nT;
    Matrix mt, nt;
    bool mb, nb;
    unsigned neighbor_id;
    if (measurement.r1 == getID()) {
      neighbor_id = measurement.r2;
      mb = getPoseInGlobalFrame(measurement.p1, mT);
      nb = getNeighborPoseInGlobalFrame(measurement.r2, measurement.p2, nT);
    } else {
      neighbor_id = measurement.r1;
      mb = getPoseInGlobalFrame(measurement.p2, mT);
      nb = getNeighborPoseInGlobalFrame(measurement.r1, measurement.p1, nT);
    }
    if (mb && nb) {
      mt = mT.block(0, d, d, 1);
      nt = nT.block(0, d, d, 1);
      geometry_msgs::msg::Point mp, np;
      mp.x = mt(0);
      mp.y = mt(1);
      mp.z = mt(2);
      np.x = nt(0);
      np.y = nt(1);
      np.z = nt(2);
      line_list.points.push_back(mp);
      line_list.points.push_back(np);
      std_msgs::msg::ColorRGBA line_color;
      line_color.a = 1;
      if (!isRobotActive(neighbor_id)) {
        // Black
      } else if (measurement.weight > 1 - weight_tol) {
        line_color.g = 1;
      } else if (measurement.weight < weight_tol) {
        line_color.r = 1;
      } else {
        line_color.b = 1;
      }
      line_list.colors.push_back(line_color);
      line_list.colors.push_back(line_color);
    }
  }
  if (!line_list.points.empty())
    mCachedLoopClosureMarkers.emplace(line_list);
}

void PGOAgentROS::publishLoopClosureMarkers() {
  if (!mParamsROS.visualizeLoopClosures) {
    return;
  }
  if (mCachedLoopClosureMarkers.has_value())
    mLoopClosureMarkerPublisher->publish(mCachedLoopClosureMarkers.value());
}

bool PGOAgentROS::createIterationLog(const std::string &filename) {
  if (mIterationLog.is_open())
    mIterationLog.close();
  mIterationLog.open(filename);
  if (!mIterationLog.is_open()) {
    RCLCPP_ERROR_STREAM(node_->get_logger(),
                        "Error opening log file: " << filename);
    return false;
  }
  // Robot ID, Cluster ID, global iteration number, Number of poses, total bytes
  // received, iteration time (sec), total elapsed time (sec), relative change
  mIterationLog << "robot_id, cluster_id, num_active_robots, iteration, "
                   "num_poses, bytes_received, "
                   "iter_time_sec, total_time_sec, rel_change \n";
  mIterationLog.flush();
  return true;
}

bool PGOAgentROS::logIteration() {
  if (!mParams.logData) {
    return false;
  }
  if (!mIterationLog.is_open()) {
    RCLCPP_ERROR_STREAM(node_->get_logger(), "No iteration log file!");
    return false;
  }

  // Compute total elapsed time since beginning of optimization
  double globalElapsedSec = (node_->now() - mGlobalStartTime).seconds();

  // Robot ID, Cluster ID, global iteration number, Number of poses, total bytes
  // received, iteration time (sec), total elapsed time (sec), relative change
  mIterationLog << getID() << ",";
  mIterationLog << getClusterID() << ",";
  mIterationLog << numActiveRobots() << ",";
  mIterationLog << iteration_number() << ",";
  mIterationLog << num_poses() << ",";
  mIterationLog << mTotalBytesReceived << ",";
  mIterationLog << mIterationElapsedMs / 1e3 << ",";
  mIterationLog << globalElapsedSec << ",";
  mIterationLog << mStatus.relativeChange << "\n";
  mIterationLog.flush();
  return true;
}

bool PGOAgentROS::logString(const std::string &str) {
  if (!mParams.logData) {
    return false;
  }
  if (!mIterationLog.is_open()) {
    RCLCPP_WARN_STREAM(node_->get_logger(), "No iteration log file!");
    return false;
  }
  mIterationLog << str << "\n";
  mIterationLog.flush();
  return true;
}

void PGOAgentROS::connectivityCallback(
    const std_msgs::msg::UInt16MultiArray::SharedPtr msg) {
  std::set<unsigned> connected_ids(msg->data.begin(), msg->data.end());
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (robot_id == getID()) {
      mTeamConnected[robot_id] = true;
    } else if (connected_ids.find(robot_id) != connected_ids.end()) {
      mTeamConnected[robot_id] = true;
    } else {
      // RCLCPP_WARN(node_->get_logger(),"Robot %u is disconnected.", robot_id);
      mTeamConnected[robot_id] = false;
    }
  }
}

void PGOAgentROS::liftingMatrixCallback(const MatrixMsg::SharedPtr msg) {
  // if (mParams.verbose) {
  //   RCLCPP_INFO(node_->get_logger(),"Robot %u receives lifting matrix.",
  //   getID());
  // }
  setLiftingMatrix(MatrixFromMsg(*msg));
}

void PGOAgentROS::anchorCallback(const PublicPoses::SharedPtr msg) {
  if (msg->robot_id != 0 || msg->pose_ids[0] != 0) {
    RCLCPP_ERROR(node_->get_logger(), "Received wrong pose as anchor!");
    return;
  }
  if (msg->cluster_id != getClusterID()) {
    return;
  }
  setGlobalAnchor(MatrixFromMsg(msg->poses[0]));
  // Print anchor error
  // if (YLift.has_value() && globalAnchor.has_value()) {
  //   const Matrix Ya = globalAnchor.value().rotation();
  //   const Matrix pa = globalAnchor.value().translation();
  //   double anchor_rotation_error = (Ya - YLift.value()).norm();
  //   double anchor_translation_error = pa.norm();
  //   RCLCPP_INFO(node_->get_logger(),"Anchor rotation error=%.1e, translation
  //   error=%.1e.", anchor_rotation_error, anchor_translation_error);
  // }
}

void PGOAgentROS::statusCallback(const Status::SharedPtr msg) {
  const auto &received_msg = *msg;
  const auto &it = mTeamStatusMsg.find(msg->robot_id);
  // Ignore message with outdated timestamp
  if (it != mTeamStatusMsg.end()) {
    const auto latest_msg = it->second;
    if (rclcpp::Time(latest_msg.header.stamp) >
        rclcpp::Time(received_msg.header.stamp)) {
      RCLCPP_WARN(node_->get_logger(),
                  "Received outdated status from robot %u.", msg->robot_id);
      return;
    }
  }
  mTeamStatusMsg[msg->robot_id] = received_msg;

  setRobotClusterID(msg->robot_id, msg->cluster_id);
  if (msg->cluster_id == getClusterID()) {
    setNeighborStatus(statusFromMsg(received_msg));
  }

  // Edge cases in synchronous mode
  if (!mParams.asynchronous) {
    if (isLeader() && isRobotActive(msg->robot_id)) {
      bool should_deactivate = false;
      if (msg->cluster_id != getClusterID()) {
        RCLCPP_WARN(node_->get_logger(),
                    "Robot %u joined other cluster %u... set to inactive.",
                    msg->robot_id, msg->cluster_id);
        should_deactivate = true;
      }
      if (iteration_number() > 0 && msg->state != Status::INITIALIZED) {
        RCLCPP_WARN(
            node_->get_logger(),
            "Robot %u is no longer initialized in global frame... set to "
            "inactive.",
            msg->robot_id);
        should_deactivate = true;
      }
      if (should_deactivate) {
        setRobotActive(msg->robot_id, false);
        publishActiveRobotsCommand();
      }
    }
  }
}

void PGOAgentROS::commandCallback(const Command::SharedPtr msg) {
  if (msg->cluster_id != getClusterID()) {
    RCLCPP_WARN_THROTTLE(
        node_->get_logger(), *node_->get_clock(), 1000,
        "Ignore command from wrong cluster (recv %u, expect %u).",
        msg->cluster_id, getClusterID());
    return;
  }
  // Update latest command time
  // Ignore commands that are periodically published
  if (msg->command != Command::NOOP &&
      msg->command != Command::SET_ACTIVE_ROBOTS) {
    mLastCommandTime = node_->now();
  }

  switch (msg->command) {
  case Command::REQUEST_POSE_GRAPH: {
    if (msg->publishing_robot != getClusterID()) {
      RCLCPP_WARN(node_->get_logger(),
                  "Ignore REQUEST_POSE_GRAPH command from non-leader %u.",
                  msg->publishing_robot);
      return;
    }
    RCLCPP_INFO(node_->get_logger(),
                "Robot %u received REQUEST_POSE_GRAPH command.", getID());
    if (mState != PGOAgentState::WAIT_FOR_DATA) {
      RCLCPP_WARN(node_->get_logger(),
                  "Robot %u status is not WAIT_FOR_DATA. Reset...", getID());
      reset();
    }
    // Update local record of currently active robots
    updateActiveRobots(msg);
    // Request latest pose graph
    bool received_pose_graph = requestPoseGraph();
    // Create log file for new round
    if (mParams.logData && received_pose_graph) {
      auto time_since_launch = node_->now() - mLaunchTime;
      int sec_since_launch = int(time_since_launch.seconds());
      std::string log_path = mParams.logDirectory + "dpgo_log_" +
                             std::to_string(sec_since_launch) + ".csv";
      createIterationLog(log_path);
    }
    publishStatus();
    // Enter initialization round
    if (isLeader()) {
      if (!received_pose_graph) {
        publishHardTerminateCommand();
      } else {
        publishAnchor();
        publishInitializeCommand();
      }
    }
    break;
  }

  case Command::TERMINATE: {
    RCLCPP_INFO(node_->get_logger(), "Robot %u received TERMINATE command. ",
                getID());
    if (!isRobotActive(getID())) {
      reset();
      break;
    }
    logString("TERMINATE");
    // When running distributed GNC, fix loop closures that have converged
    if (mParams.robustCostParams.costType ==
        RobustCostParameters::Type::GNC_TLS) {
      double residual = 0;
      double weight = 0;
      for (auto &m : mPoseGraph->activeLoopClosures()) {
        if (!m->fixedWeight && computeMeasurementResidual(*m, &residual)) {
          weight = mRobustCost.weight(residual);
          if (weight < mParamsROS.weightConvergenceThreshold) {
            RCLCPP_INFO(node_->get_logger(),
                        "Reject measurement with residual %f and weight %f.",
                        residual, weight);
            m->weight = 0;
            m->fixedWeight = true;
          }
        }
      }
      const auto stat = mPoseGraph->statistics();
      RCLCPP_INFO(node_->get_logger(),
                  "Robot %u loop closure statistics:\n "
                  "accepted: %f\n "
                  "rejected: %f\n "
                  "undecided: %f\n",
                  mID, stat.accept_loop_closures, stat.reject_loop_closures,
                  stat.undecided_loop_closures);
      publishMeasurementWeights();
    }

    // Store and publish optimized trajectory in global frame
    storeOptimizedTrajectory();
    storeLoopClosureMarkers();
    storeActiveNeighborPoses();
    storeActiveEdgeWeights();

    randomSleep(0.1, 5);
    publishOptimizedTrajectory();
    publishLoopClosureMarkers();
    reset();
    break;
  }

  case Command::HARD_TERMINATE: {
    RCLCPP_INFO(node_->get_logger(),
                "Robot %u received HARD TERMINATE command. ", getID());
    logString("HARD_TERMINATE");
    reset();
    break;
  }

  case Command::INITIALIZE: {
    // TODO: ignore if status == WAIT_FOR_DATA
    if (msg->publishing_robot != getClusterID()) {
      RCLCPP_WARN(node_->get_logger(),
                  "Ignore INITIALIZE command from non-leader %u.",
                  msg->publishing_robot);
      return;
    }
    mGlobalStartTime = node_->now();
    publishPublicMeasurements();
    publishPublicPoses(false);
    publishStatus();
    if (isLeader()) {
      publishLiftingMatrix();
      // updateActiveRobots();
      publishActiveRobotsCommand();
      // sleep 0.1 s using ros2
      rclcpp::sleep_for(std::chrono::nanoseconds(
          static_cast<int>(0.1 * 1e9))); // Sleep for 0.1 second

      // Check the status of all robots
      bool all_initialized = true;
      int num_initialized_robots = 0;
      for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
        if (!isRobotActive(robot_id)) {
          // Ignore inactive robots
          continue;
        }
        if (!hasNeighborStatus(robot_id)) {
          RCLCPP_WARN(node_->get_logger(), "Robot %u status not available.",
                      robot_id);
          all_initialized = false;
          continue;
        }
        const auto status = getNeighborStatus(robot_id);
        if (status.state == PGOAgentState::WAIT_FOR_DATA) {
          RCLCPP_WARN(node_->get_logger(),
                      "Robot %u has not received pose graph.", status.agentID);
          all_initialized = false;
        } else if (status.state == PGOAgentState::WAIT_FOR_INITIALIZATION) {
          RCLCPP_WARN(node_->get_logger(),
                      "Robot %u has not initialized in global frame.",
                      status.agentID);
          all_initialized = false;
        } else if (status.state == PGOAgentState::INITIALIZED) {
          num_initialized_robots++;
        }
      }

      if (!all_initialized &&
          mInitStepsDone <= mParamsROS.maxDistributedInitSteps) {
        // Keep waiting for more robots to initialize
        mPublishInitializeCommandRequested = true;
        return;
      } else {
        // Start distributed optimization if more than 1 robot is initialized
        if (num_initialized_robots > 1) {
          RCLCPP_INFO(
              node_->get_logger(),
              "Start distributed optimization with %i/%zu active robots.",
              num_initialized_robots, numActiveRobots());
          // Set robots that are not initialized to inactive
          for (unsigned int robot_id = 0; robot_id < mParams.numRobots;
               ++robot_id) {
            if (isRobotActive(robot_id) && isRobotInitialized(robot_id) &&
                isRobotConnected(robot_id)) {
              setRobotActive(robot_id, true);
            } else {
              setRobotActive(robot_id, false);
            }
          }
          publishActiveRobotsCommand();
          publishUpdateCommand(getID()); // Kick off optimization
        } else {
          RCLCPP_WARN(node_->get_logger(), "Not enough robots initialized.");
          publishHardTerminateCommand();
        }
      }
    }
    break;
  }

  case Command::UPDATE: {
    CHECK(!mParams.asynchronous);
    // Handle case when this robot is not active
    if (!isRobotActive(getID())) {
      RCLCPP_WARN_STREAM(
          node_->get_logger(),
          "Robot " << getID() << " is deactivated. Ignore update command... ");
      return;
    }
    // Handle edge case when robots are out of sync
    if (mState != PGOAgentState::INITIALIZED) {
      RCLCPP_WARN_STREAM(
          node_->get_logger(),
          "Robot " << getID()
                   << " is not initialized. Ignore update command...");
      return;
    }
    // Update local record
    mTeamIterRequired[msg->executing_robot] = msg->executing_iteration;
    if (msg->executing_iteration != iteration_number() + 1) {
      RCLCPP_WARN(node_->get_logger(),
                  "Update iteration does not match local iteration. (received: "
                  "%u, local: %u)",
                  msg->executing_iteration, iteration_number() + 1);
    }
    if (msg->executing_robot == getID()) {
      mSynchronousOptimizationRequested = true;
      if (mParams.verbose)
        RCLCPP_INFO(node_->get_logger(), "Robot %u to update at iteration %u.",
                    getID(), msg->executing_iteration);
    } else {
      // Agents that are not selected for optimization can iterate immediately
      iterate(false);
      publishStatus();
    }
    break;
  }

  case Command::RECOVER: {
    CHECK(!mParams.asynchronous);
    if (!isRobotActive(getID()) || mState != PGOAgentState::INITIALIZED) {
      return;
    }
    mIterationNumber = msg->executing_iteration;
    mSynchronousOptimizationRequested = false;
    for (const auto &neighbor : getNeighbors()) {
      mTeamIterRequired[neighbor] = iteration_number();
      mTeamIterReceived[neighbor] =
          0; // Force robot to wait for updated public poses from neighbors
    }
    RCLCPP_WARN(
        node_->get_logger(),
        "Robot %u received RECOVER command and reset iteration number to %u.",
        getID(), iteration_number());

    if (isLeader()) {
      RCLCPP_WARN(node_->get_logger(), "Leader %u publishes update command.",
                  getID());
      publishUpdateCommand(getID());
    }
    break;
  }

  case Command::UPDATE_WEIGHT: {
    CHECK(!mParams.asynchronous);
    if (!isRobotActive(getID())) {
      RCLCPP_WARN_STREAM(
          node_->get_logger(),
          "Robot " << getID()
                   << " is deactivated. Ignore UPDATE_WEIGHT command... ");
      return;
    }
    logString("UPDATE_WEIGHT");
    updateMeasurementWeights();
    // Require latest iterations from all neighbor robots
    RCLCPP_WARN(node_->get_logger(),
                "Require latest iteration %d from all neighbors.",
                iteration_number());
    for (const auto &neighbor : getNeighbors()) {
      mTeamIterRequired[neighbor] = iteration_number();
    }
    publishMeasurementWeights();
    publishPublicPoses(false);
    if (mParams.acceleration)
      publishPublicPoses(true);
    publishStatus();
    // The first resumes optimization by sending UPDATE command
    if (isLeader()) {
      publishUpdateCommand();
    }
    break;
  }

  case Command::SET_ACTIVE_ROBOTS: {
    if (msg->publishing_robot != getClusterID()) {
      RCLCPP_WARN(node_->get_logger(),
                  "Ignore SET_ACTIVE_ROBOTS command from non-leader %u.",
                  msg->publishing_robot);
      return;
    }
    // Update local record of currently active robots
    updateActiveRobots(msg);
    break;
  }

  case Command::NOOP: {
    // Do nothing
    break;
  }

  default:
    RCLCPP_ERROR(node_->get_logger(), "Invalid command!");
  }
}

void PGOAgentROS::publicPosesCallback(const PublicPoses::SharedPtr msg) {

  // Discard message sent by robots in other clusters
  if (msg->cluster_id != getClusterID()) {
    return;
  }

  std::vector<unsigned> neighbors = getNeighbors();
  if (std::find(neighbors.begin(), neighbors.end(), msg->robot_id) ==
      neighbors.end()) {
    // Discard messages send by non-neighbors
    return;
  }

  // Generate a random permutation of indices
  PoseDict poseDict;
  for (size_t index = 0; index < msg->pose_ids.size(); ++index) {
    const PoseID nID(msg->robot_id, msg->pose_ids.at(index));
    const auto matrix = MatrixFromMsg(msg->poses.at(index));
    poseDict.emplace(nID, matrix);
  }
  if (!msg->is_auxiliary) {
    updateNeighborPoses(msg->robot_id, poseDict);
  } else {
    updateAuxNeighborPoses(msg->robot_id, poseDict);
  }

  // Update local bookkeeping
  mTeamIterReceived[msg->robot_id] = msg->iteration_number;
  mTotalBytesReceived += computePublicPosesMsgSize(*msg);
}

void PGOAgentROS::publicMeasurementsCallback(
    const RelativeMeasurementList::SharedPtr msg) {
  // Ignore if message not addressed to this robot
  if (msg->to_robot != getID()) {
    return;
  }
  // Ignore if does not have local odometry
  if (mPoseGraph->numOdometry() == 0)
    return;
  // Ignore if already received inter-robot loop closures from this robot
  if (mTeamReceivedSharedLoopClosures[msg->from_robot])
    return;
  // Ignore if from another cluster
  if (msg->from_cluster != getClusterID())
    return;
  mTeamReceivedSharedLoopClosures[msg->from_robot] = true;

  // Add inter-robot loop closures that involve this robot
  const auto num_before = mPoseGraph->numSharedLoopClosures();
  for (const auto &e : msg->edges) {
    if (e.robot_from == (int)getID() || e.robot_to == (int)getID()) {
      const auto measurement = RelativeMeasurementFromMsg(e);
      addMeasurement(measurement);
    }
  }
  const auto num_after = mPoseGraph->numSharedLoopClosures();
  RCLCPP_INFO(node_->get_logger(),
              "Robot %u received measurements from %u: "
              "added %u missing measurements.",
              getID(), msg->from_robot, num_after - num_before);
}

void PGOAgentROS::measurementWeightsCallback(
    const RelativeMeasurementWeights::SharedPtr msg) {
  // if (mState != PGOAgentState::INITIALIZED) return;
  if (msg->destination_robot_id != getID())
    return;
  if (msg->cluster_id != getClusterID())
    return;
  bool weights_updated = false;
  for (size_t k = 0; k < msg->weights.size(); ++k) {
    const unsigned robotSrc = msg->src_robot_ids[k];
    const unsigned robotDst = msg->dst_robot_ids[k];
    const unsigned poseSrc = msg->src_pose_ids[k];
    const unsigned poseDst = msg->dst_pose_ids[k];
    const PoseID srcID(robotSrc, poseSrc);
    const PoseID dstID(robotDst, poseDst);
    double w = msg->weights[k];
    bool fixed = msg->fixed_weights[k];

    unsigned otherID;
    if (robotSrc == getID() && robotDst != getID()) {
      otherID = robotDst;
    } else if (robotDst == getID() && robotSrc != getID()) {
      otherID = robotSrc;
    } else {
      RCLCPP_ERROR(node_->get_logger(),
                   "Received weight for irrelevant measurement!");
      continue;
    }
    if (!isRobotActive(otherID))
      continue;
    if (otherID < getID()) {
      if (setMeasurementWeight(srcID, dstID, w, fixed))
        weights_updated = true;
      else {
        RCLCPP_WARN(
            node_->get_logger(),
            "Cannot find specified shared loop closure (%u, %u) -> (%u, %u)",
            robotSrc, poseSrc, robotDst, poseDst);
      }
    }
  }
  if (weights_updated) {
    // Need to recompute data matrices in the pose graph
    mPoseGraph->clearDataMatrices();
  }
}

void PGOAgentROS::timerCallback() {
  publishNoopCommand();
  publishLiftingMatrix();
  if (mPublishInitializeCommandRequested) {
    publishInitializeCommand();
  }
  if (mTryInitializeRequested) {
    tryInitialize();
  }
  if (mState == PGOAgentState::WAIT_FOR_DATA) {
    // Update leader robot when idle
    updateCluster();
    // Initialize a new round of dpgo
    int elapsed_sec = (node_->now() - mLastResetTime).seconds();
    if (isLeader() && elapsed_sec > 10) {
      RCLCPP_INFO(node_->get_logger(),
                  "Leader robot %u requests new pose graph.", getID());
      // request for pose graph if idle for 10 seconds
      publishRequestPoseGraphCommand();
    }
  }
  if (mState == PGOAgentState::INITIALIZED) {
    publishPublicPoses(false);
    if (mParamsROS.acceleration)
      publishPublicPoses(true);
    publishMeasurementWeights();
    if (isLeader()) {
      publishAnchor();
      publishActiveRobotsCommand();
    }
  }
  publishStatus();
}

void PGOAgentROS::visualizationTimerCallback() {
  publishOptimizedTrajectory();
  publishLoopClosureMarkers();
}

void PGOAgentROS::storeActiveNeighborPoses() {
  Matrix matrix;
  int num_poses_stored = 0;
  for (const auto &nbr_pose_id : mPoseGraph->activeNeighborPublicPoseIDs()) {
    if (getNeighborPoseInGlobalFrame(nbr_pose_id.robot_id, nbr_pose_id.frame_id,
                                     matrix)) {
      Pose T(dimension());
      T.setData(matrix);
      mCachedNeighborPoses[nbr_pose_id] = T;
      num_poses_stored++;
    }
  }
  RCLCPP_INFO(node_->get_logger(), "Stored %i neighbor poses in world frame.",
              num_poses_stored);
}

void PGOAgentROS::setInactiveNeighborPoses() {
  if (!YLift) {
    RCLCPP_WARN(node_->get_logger(),
                "Missing lifting matrix! Cannot apply neighbor poses.");
    return;
  }
  int num_poses_initialized = 0;
  for (const auto &it : mCachedNeighborPoses) {
    const auto &pose_id = it.first;
    // Active neighbors will transmit their poses
    // Therefore we only use stored poses for inactive neighbors
    if (!isRobotActive(pose_id.robot_id)) {
      const auto &Ti = it.second;
      Matrix Xi_mat = YLift.value() * Ti.getData();
      LiftedPose Xi(r, d);
      Xi.setData(Xi_mat);
      neighborPoseDict[pose_id] = Xi;
      num_poses_initialized++;
    }
  }
  RCLCPP_INFO(node_->get_logger(), "Set %i inactive neighbor poses.",
              num_poses_initialized);
}

void PGOAgentROS::storeActiveEdgeWeights() {
  int num_edges_stored = 0;
  for (const RelativeSEMeasurement *m : mPoseGraph->activeLoopClosures()) {
    const PoseID src_id(m->r1, m->p1);
    const PoseID dst_id(m->r2, m->p2);
    const EdgeID edge_id(src_id, dst_id);
    if (edge_id.isSharedLoopClosure()) {
      mCachedEdgeWeights[edge_id] = m->weight;
      num_edges_stored++;
    }
  }
  RCLCPP_INFO(node_->get_logger(), "Stored %i active edge weights.",
              num_edges_stored);
}

void PGOAgentROS::setInactiveEdgeWeights() {
  int num_edges_set = 0;
  for (RelativeSEMeasurement *m : mPoseGraph->inactiveLoopClosures()) {
    const PoseID src_id(m->r1, m->p1);
    const PoseID dst_id(m->r2, m->p2);
    const EdgeID edge_id(src_id, dst_id);
    const auto &it = mCachedEdgeWeights.find(edge_id);
    if (it != mCachedEdgeWeights.end()) {
      m->weight = it->second;
      num_edges_set++;
    }
  }
  RCLCPP_INFO(node_->get_logger(), "Set %i inactive edge weights.",
              num_edges_set);
}

void PGOAgentROS::initializeGlobalAnchor() {
  if (!YLift) {
    RCLCPP_WARN(node_->get_logger(),
                "Missing lifting matrix! Cannot initialize global anchor.");
    return;
  }
  LiftedPose X(r, d);
  X.rotation() = YLift.value();
  X.translation() = Vector::Zero(r);
  setGlobalAnchor(X.getData());
  RCLCPP_INFO(node_->get_logger(), "Initialized global anchor.");
}

unsigned PGOAgentROS::getClusterID() const { return mClusterID; }

bool PGOAgentROS::isLeader() const { return getID() == getClusterID(); }

void PGOAgentROS::updateCluster() {
  for (unsigned int robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (isRobotConnected(robot_id)) {
      mClusterID = robot_id;
      break;
    }
  }
  RCLCPP_INFO(node_->get_logger(), "Robot %u joins cluster %u.", getID(),
              mClusterID);
}

unsigned PGOAgentROS::getRobotClusterID(unsigned robot_id) const {
  if (robot_id > mParams.numRobots) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Robot ID %u larger than number of robots.", robot_id);
    return robot_id;
  }
  return mTeamClusterID[robot_id];
}

void PGOAgentROS::setRobotClusterID(unsigned robot_id, unsigned cluster_id) {
  if (robot_id > mParams.numRobots) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Robot ID %u larger than number of robots.", robot_id);
    return;
  }
  if (cluster_id > mParams.numRobots) {
    RCLCPP_ERROR(node_->get_logger(),
                 "Cluster ID %u larger than number of robots.", cluster_id);
    return;
  }
  mTeamClusterID[robot_id] = cluster_id;
}

void PGOAgentROS::resetRobotClusterIDs() {
  mTeamClusterID.assign(mParams.numRobots, 0);
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    mTeamClusterID[robot_id] = robot_id;
  }
}

void PGOAgentROS::checkTimeout() {
  if (mParams.asynchronous) {
    return;
  }

  // Timeout if command channel quiet for long time
  // This usually happen when robots get disconnected
  double elapsedSecond = (node_->now() - mLastCommandTime).seconds();
  if (elapsedSecond > mParamsROS.timeoutThreshold) {
    if (mState == PGOAgentState::INITIALIZED && iteration_number() > 0) {
      RCLCPP_WARN(node_->get_logger(),
                  "Robot %u timeout during optimization: last command was %.1f "
                  "sec ago.",
                  getID(), elapsedSecond);
      if (isLeader()) {
        if (checkDisconnectedRobot()) {
          publishActiveRobotsCommand();
          rclcpp::sleep_for(std::chrono::nanoseconds(
              static_cast<int>(3 * 1e9))); // Sleep for 3 second
        }
        RCLCPP_WARN(node_->get_logger(), "Number of active robots: %zu.",
                    numActiveRobots());
        if (numActiveRobots() > 1) {
          if (mParamsROS.enableRecovery) {
            // RCLCPP_WARN(node_->get_logger(),"Attempt to resume optimization
            // with %zu robots.", numActiveRobots());
            publishRecoverCommand();
          } else {
            // RCLCPP_WARN(node_->get_logger(),"Terminate with %zu robots.",
            // numActiveRobots());
            publishHardTerminateCommand();
          }
        } else {
          // RCLCPP_WARN(node_->get_logger(),"Terminate... Not enough active
          // robots.");
          publishHardTerminateCommand();
        }
      } else {
        if (!isRobotConnected(getClusterID())) {
          RCLCPP_WARN(node_->get_logger(),
                      "Disconnected from current cluster... reset.");
          reset();
        }
      }
    } else {
      reset();
      if (isLeader()) {
        publishHardTerminateCommand();
      }
    }
    mLastCommandTime = node_->now();
  }

  // Check hard timeout
  if (mState == PGOAgentState::INITIALIZED && iteration_number() > 0) {
    if (mLastUpdateTime.has_value()) {
      double sec_idle = (node_->now() - mLastUpdateTime.value()).seconds();
      if (sec_idle > 1) {
        RCLCPP_WARN_THROTTLE(node_->get_logger(), *node_->get_clock(), 1000,
                             "Robot %u last successful update is %.1f sec ago.",
                             getID(), sec_idle);
      }
      if (sec_idle > 3 * mParamsROS.timeoutThreshold) {
        RCLCPP_ERROR(node_->get_logger(), "Hard timeout!");
        logString("TIMEOUT");
        if (isLeader())
          publishHardTerminateCommand();
        reset();
      }
    }
  }
}

bool PGOAgentROS::checkDisconnectedRobot() {
  bool robot_disconnected = false;
  for (unsigned robot_id = 0; robot_id < mParams.numRobots; ++robot_id) {
    if (isRobotActive(robot_id) && !isRobotConnected(robot_id)) {
      RCLCPP_WARN(node_->get_logger(), "Active robot %u is disconnected.",
                  robot_id);
      setRobotActive(robot_id, false);
      robot_disconnected = true;
    }
  }
  return robot_disconnected;
}

} // namespace dpgo_ros
