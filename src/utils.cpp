/* ----------------------------------------------------------------------------
 * Copyright 2020, Massachusetts Institute of Technology, * Cambridge, MA 02139
 * All Rights Reserved
 * Authors: Yulun Tian, et al. (see READpose_graph_tools_msgs::msg::PoseGraph
 TrajectoryToPoseGraph(unsigned d, unsigned n, const Matrix &T, unsigned
 robotID) { assert(d == 3); assert(T.rows() == d); assert(T.cols() == (d + 1) *
 n); pose_graph_tools_msgs::msg::PoseGraph pose_graph_msg;
  pose_graph_msg.header.frame_id = "/world";
  pose_graph_msg.header.stamp = rclcpp::Clock().now();
  for (size_t i = 0; i < n; ++i) {
    pose_graph_tools_msgs::msg::PoseGraphNode node_msg;e full author list)
 * See LICENSE for the license information
 * -------------------------------------------------------------------------- */

#include <DPGO/DPGO_types.h>
#include <DPGO/DPGO_utils.h>
#include <chrono>
#include <dpgo_ros/msg/status.hpp>
#include <dpgo_ros/utils.h>
#include <map>
#include <random>
#include <rclcpp/rclcpp.hpp>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>

using namespace DPGO;
using pose_graph_tools_msgs::msg::PoseGraphEdge;

namespace dpgo_ros {

std::vector<double> serializeMatrix(size_t rows, size_t cols,
                                    const Matrix &Mat) {
  assert((size_t)Mat.rows() == rows);
  assert((size_t)Mat.cols() == cols);

  std::vector<double> v;

  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      double scalar = Mat(row, col);
      v.push_back(scalar);
    }
  }

  return v;
}

Matrix deserializeMatrix(size_t rows, size_t cols,
                         const std::vector<double> &v) {
  assert(v.size() == rows * cols);
  Matrix Mat = Matrix::Zero(rows, cols);
  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      size_t index = row * cols + col;
      Mat(row, col) = v[index];
    }
  }

  return Mat;
}

dpgo_ros::msg::MatrixMsg MatrixToMsg(const Matrix &Mat) {
  dpgo_ros::msg::MatrixMsg msg;
  msg.rows = Mat.rows();
  msg.cols = Mat.cols();
  msg.values = serializeMatrix(msg.rows, msg.cols, Mat);
  return msg;
}

Matrix MatrixFromMsg(const dpgo_ros::msg::MatrixMsg &msg) {
  return deserializeMatrix(msg.rows, msg.cols, msg.values);
}

Matrix RotationFromPoseMsg(const geometry_msgs::msg::Pose &msg) {
  // read rotation
  tf2::Quaternion quat;
  tf2::fromMsg(msg.orientation, quat);
  tf2::Matrix3x3 rotation(quat);
  Matrix R(3, 3);
  R << rotation[0][0], rotation[0][1], rotation[0][2], rotation[1][0],
      rotation[1][1], rotation[1][2], rotation[2][0], rotation[2][1],
      rotation[2][2];

  return R;
}

Matrix TranslationFromPoseMsg(const geometry_msgs::msg::Pose &msg) {
  tf2::Vector3 translation;
  tf2::fromMsg(msg.position, translation);
  Matrix t(3, 1);
  t << translation.x(), translation.y(), translation.z();
  return t;
}

geometry_msgs::msg::Quaternion RotationToQuaternionMsg(const Matrix &R) {
  assert(R.rows() == 3);
  assert(R.cols() == 3);

  tf2::Matrix3x3 rotation(R(0, 0), R(0, 1), R(0, 2), R(1, 0), R(1, 1), R(1, 2),
                          R(2, 0), R(2, 1), R(2, 2));
  tf2::Quaternion quat;
  rotation.getRotation(quat);
  geometry_msgs::msg::Quaternion quat_msg;
  quat_msg = tf2::toMsg(quat);
  return quat_msg;
}

geometry_msgs::msg::Point TranslationToPointMsg(const Matrix &t) {
  assert(t.rows() == 3);
  assert(t.cols() == 1);
  // convert translation to ROS message
  tf2::Vector3 translation(t(0), t(1), t(2));
  geometry_msgs::msg::Point point_msg;
  point_msg.x = translation.x();
  point_msg.y = translation.y();
  point_msg.z = translation.z();
  return point_msg;
}

PoseGraphEdge RelativeMeasurementToMsg(const RelativeSEMeasurement &m) {
  assert(m.R.rows() == 3 && m.R.cols() == 3);
  assert(m.t.rows() == 3 && m.t.cols() == 1);

  PoseGraphEdge msg;
  msg.robot_from = m.r1;
  msg.robot_to = m.r2;
  msg.key_from = m.p1;
  msg.key_to = m.p2;

  // convert rotation to ROS message
  msg.pose.orientation = RotationToQuaternionMsg(m.R);

  // convert translation to ROS message
  msg.pose.position = TranslationToPointMsg(m.t);

  // TODO: write covariance to message
  return msg;
}

RelativeSEMeasurement RelativeMeasurementFromMsg(const PoseGraphEdge &msg) {
  size_t r1 = msg.robot_from;
  size_t r2 = msg.robot_to;
  size_t p1 = msg.key_from;
  size_t p2 = msg.key_to;

  // read rotation
  Matrix R = RotationFromPoseMsg(msg.pose);

  // read translation
  Matrix t = TranslationFromPoseMsg(msg.pose);

  // TODO: read covariance from message
  double kappa = 10000;
  double tau = 100;

  RelativeSEMeasurement m(r1, r2, p1, p2, R, t, kappa, tau);

  // By default, odometry edge is inlier
  if (r1 == r2 && p1 + 1 == p2) {
    m.fixedWeight = true;
  }

  return m;
}

geometry_msgs::msg::PoseArray TrajectoryToPoseArray(unsigned d, unsigned n,
                                                    const Matrix &T) {
  assert(d == 3);
  assert(T.rows() == d);
  assert(T.cols() == (d + 1) * n);
  geometry_msgs::msg::PoseArray msg;
  msg.header.frame_id = "/world";
  msg.header.stamp = rclcpp::Clock().now();
  for (size_t i = 0; i < n; ++i) {
    geometry_msgs::msg::Pose pose;
    Matrix Ri = T.block(0, i * (d + 1), d, d);
    Matrix ti = T.block(0, i * (d + 1) + d, d, 1);

    // convert rotation to ROS message
    pose.orientation = RotationToQuaternionMsg(Ri);

    // convert translation to ROS message
    pose.position = TranslationToPointMsg(ti);

    msg.poses.push_back(pose);
  }
  return msg;
}

nav_msgs::msg::Path TrajectoryToPath(unsigned d, unsigned n, const Matrix &T) {
  assert(d == 3);
  assert(T.rows() == d);
  assert(T.cols() == (d + 1) * n);
  nav_msgs::msg::Path msg;
  msg.header.frame_id = "/world";
  msg.header.stamp = rclcpp::Clock().now();
  for (size_t i = 0; i < n; ++i) {
    geometry_msgs::msg::Pose pose;
    Matrix Ri = T.block(0, i * (d + 1), d, d);
    Matrix ti = T.block(0, i * (d + 1) + d, d, 1);

    // convert rotation to ROS message
    pose.orientation = RotationToQuaternionMsg(Ri);

    // convert translation to ROS message
    pose.position = TranslationToPointMsg(ti);

    geometry_msgs::msg::PoseStamped poseStamped;
    poseStamped.header.frame_id = "/world";
    poseStamped.header.stamp = rclcpp::Clock().now();
    poseStamped.pose = pose;

    msg.poses.push_back(poseStamped);
  }
  return msg;
}

sensor_msgs::msg::PointCloud TrajectoryToPointCloud(unsigned d, unsigned n,
                                                    const Matrix &T) {
  assert(d == 3);
  assert(T.rows() == d);
  assert(T.cols() == (d + 1) * n);
  sensor_msgs::msg::PointCloud msg;
  msg.header.frame_id = "/world";
  msg.header.stamp = rclcpp::Clock().now();
  for (size_t i = 0; i < n; ++i) {
    geometry_msgs::msg::Point32 point;
    Matrix ti = T.block(0, i * (d + 1) + d, d, 1);
    point.x = ti(0);
    point.y = ti(1);
    point.z = ti(2);
    msg.points.push_back(point);
  }
  return msg;
}

pose_graph_tools_msgs::msg::PoseGraph
TrajectoryToPoseGraphMsg(unsigned robotID, unsigned d, unsigned n,
                         const Matrix &T) {
  assert(d == 3);
  assert(T.rows() == d);
  assert(T.cols() == (d + 1) * n);
  pose_graph_tools_msgs::msg::PoseGraph pose_graph_msg;
  pose_graph_msg.header.frame_id = "/world";
  pose_graph_msg.header.stamp = rclcpp::Clock().now();
  for (size_t i = 0; i < n; ++i) {
    pose_graph_tools_msgs::msg::PoseGraphNode node_msg;
    node_msg.robot_id = robotID;
    node_msg.key = i;
    node_msg.header.frame_id = "/world";
    node_msg.header.stamp = pose_graph_msg.header.stamp;

    Matrix Ri = T.block(0, i * (d + 1), d, d);
    Matrix ti = T.block(0, i * (d + 1) + d, d, 1);

    // convert rotation to ROS message
    node_msg.pose.orientation = RotationToQuaternionMsg(Ri);

    // convert translation to ROS message
    node_msg.pose.position = TranslationToPointMsg(ti);

    pose_graph_msg.nodes.push_back(node_msg);
  }
  return pose_graph_msg;
}

size_t computePublicPosesMsgSize(const msg::PublicPoses &msg) {
  size_t bytes = 0;
  bytes += sizeof(msg.robot_id);
  bytes += sizeof(msg.instance_number);
  bytes += sizeof(msg.iteration_number);
  bytes += sizeof(msg.is_auxiliary);
  bytes += sizeof(msg.pose_ids[0]) * msg.pose_ids.size();
  bytes += sizeof(msg.poses[0]) * msg.poses.size();
  return bytes;
}

msg::Status statusToMsg(const PGOAgentStatus &status) {
  msg::Status msg;
  msg.robot_id = status.agentID;
  msg.state = status.state;
  msg.instance_number = status.instanceNumber;
  msg.iteration_number = status.iterationNumber;
  msg.ready_to_terminate = status.readyToTerminate;
  msg.relative_change = status.relativeChange;
  return msg;
}

PGOAgentStatus statusFromMsg(const msg::Status &msg) {
  PGOAgentStatus status(msg.robot_id, static_cast<PGOAgentState>(msg.state),
                        msg.instance_number, msg.iteration_number,
                        msg.ready_to_terminate, msg.relative_change);
  return status;
}

void randomSleep(double min_sec, double max_sec) {
  CHECK(min_sec < max_sec);
  CHECK(min_sec > 0);
  if (max_sec < 1e-3)
    return;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<double> distribution(min_sec, max_sec);
  double sleep_time = distribution(gen);
  RCLCPP_INFO(rclcpp::get_logger("dpgo_ros"), "Sleep %f sec...", sleep_time);
  std::chrono::nanoseconds duration(static_cast<long>(sleep_time * 1e9));
  rclcpp::sleep_for(duration);
}

} // namespace dpgo_ros