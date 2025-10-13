/* ----------------------------------------------------------------------------
 * Copyright 2020, Massachusetts Institute of Technology, * Cambridge, MA 02139
 * All Rights Reserved
 * Authors: Yulun Tian, et al. (see README for the full author list)
 * See LICENSE for the license information
 * -------------------------------------------------------------------------- */

#pragma once

#include <DPGO/DPGO_types.h>
#include <DPGO/PGOAgent.h>
#include <DPGO/RelativeSEMeasurement.h>

#ifdef PI
  #undef PI
#endif

#include <dpgo_ros/msg/matrix_msg.hpp>
#include <dpgo_ros/msg/public_poses.hpp>
#include <dpgo_ros/msg/status.hpp>
#include <geometry_msgs/msg/pose_array.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <geometry_msgs/msg/pose.hpp>
#include <geometry_msgs/msg/quaternion.hpp>
#include <geometry_msgs/msg/point.hpp>
#include <sensor_msgs/msg/point_cloud.hpp>
#include <nav_msgs/msg/path.hpp>
#include <pose_graph_tools_msgs/msg/pose_graph.hpp>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>

#include <cassert>
#include <fstream>
#include <vector>

using namespace DPGO;
using pose_graph_tools_msgs::msg::PoseGraphEdge;

namespace dpgo_ros {

/**
Serialize a Matrix object into a vector of Float64 messages, in row-major format
*/
std::vector<double> serializeMatrix(size_t rows, size_t cols,
                                    const Matrix &Mat);

/**
Deserialize a vector of Float64 messages into a Matrix object, using row-major
format
*/
Matrix deserializeMatrix(size_t rows, size_t cols,
                         const std::vector<double> &v);

/**
Write a matrix to ROS message
*/
dpgo_ros::msg::MatrixMsg MatrixToMsg(const Matrix &Mat);

/**
Read a matrix from ROS message
*/
Matrix MatrixFromMsg(const dpgo_ros::msg::MatrixMsg &msg);

/**
 * @brief Retrieve 3-by-3 rotation matrix from geometry_msgs::Pose
 * @param msg
 * @return
 */
Matrix RotationFromPoseMsg(const geometry_msgs::msg::Pose &msg);

/**
 * @brief Retrieve 3-by-1 translation vector from geometry_msgs::Pose
 * @param msg
 * @return
 */
Matrix TranslationFromPoseMsg(const geometry_msgs::msg::Pose &msg);

/**
 * @brief Convert a 3-by-3 rotation matrix to quaternion message in ROS
 * @param R
 * @return
 */
geometry_msgs::msg::Quaternion RotationToQuaternionMsg(const Matrix &R);

/**
 * @brief Convert a 3-by-1 translation vector to point message in ROS
 * @param t
 * @return
 */
geometry_msgs::msg::Point TranslationToPointMsg(const Matrix &t);

/**
Write a relative measurement to ROS message
*/
PoseGraphEdge RelativeMeasurementToMsg(const RelativeSEMeasurement &m);

/**
Read a relative measurement from ROS message
*/
RelativeSEMeasurement RelativeMeasurementFromMsg(const PoseGraphEdge &msg);

/**
Convert an aggregate matrix T \in (SO(d) \times Rd)^n to a ROS PoseArray message
*/
geometry_msgs::msg::PoseArray TrajectoryToPoseArray(unsigned d, unsigned n, const Matrix &T);

/**
Convert an aggregate matrix T \in (SO(d) \times Rd)^n to a ROS Path message
*/
nav_msgs::msg::Path TrajectoryToPath(unsigned d, unsigned n, const Matrix &T);

/**
 * @brief Convert an an aggregate matrix T \in (SO(d) \times Rd)^n to a point cloud message. This message does not contain rotation estimates.
 * @param d
 * @param n
 * @param T
 * @return
 */
sensor_msgs::msg::PointCloud TrajectoryToPointCloud(unsigned d, unsigned n, const Matrix &T);

/**
 * @brief Convert an aggregate matrix T \in (SO(d) \times Rd)^n to a PoseGraph message. Currently only populates the nodes.
 * @param robotID
 * @param d
 * @param n
 * @param T
 * @return
 */
pose_graph_tools_msgs::msg::PoseGraph TrajectoryToPoseGraphMsg(unsigned robotID, unsigned d, unsigned n, const Matrix &T);

/**
Compute the number of bytes of a PublicPoses message.
*/
size_t computePublicPosesMsgSize(const dpgo_ros::msg::PublicPoses &msg);

/**
 * @brief Convert a PGOAgentStatus struct to its corresponding ROS message
 * @param status
 * @return
 */
dpgo_ros::msg::Status statusToMsg(const PGOAgentStatus &status);

/**
 * @brief Create a PGOAgentStatus struct from its corresponding ROS message
 * @param msg
 * @return
 */
PGOAgentStatus statusFromMsg(const dpgo_ros::msg::Status &msg);

/**
 * @brief Sleep for a time randomly distributed in [min_sec, max_sec]
 */
void randomSleep(double min_sec, double max_sec);

}  // namespace dpgo_ros
