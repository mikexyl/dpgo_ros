/* ----------------------------------------------------------------------------
 * Copyright 2020, Massachusetts Institute of Technology, * Cambridge, MA 02139
 * All Rights Reserved
 * Authors: Yulun Tian, et al. (see README for the full author list)
 * See LICENSE for the license information
 * -------------------------------------------------------------------------- */

#include <DPGO/DPGO_utils.h>
#include <dpgo_ros/utils.h>
#include <pose_graph_tools_msgs/msg/pose_graph.hpp>
#include <pose_graph_tools_msgs/srv/pose_graph_query.hpp>
#include <rclcpp/rclcpp.hpp>

#include <functional>
#include <map>
#include <vector>

using std::map;
using std::string;
using std::vector;
using namespace DPGO;

class DatasetPublisher : public rclcpp::Node {
public:
  DatasetPublisher() : Node("dataset_publisher_node"), num_robots(0) {
    this->declare_parameter("num_robots", 0);
    if (!this->get_parameter("num_robots", num_robots)) {
      RCLCPP_ERROR_STREAM(this->get_logger(),
                          "Failed to get number of robots!");
    }

    // Load robot names
    for (size_t id = 0; id < (unsigned)num_robots; id++) {
      std::string robot_name = "kimera" + std::to_string(id);
      this->declare_parameter("robot" + std::to_string(id) + "_name",
                              robot_name);
      this->get_parameter("robot" + std::to_string(id) + "_name", robot_name);
      robotNames[id] = robot_name;
    }

    string filename;
    this->declare_parameter("g2o_file", std::string(""));
    if (this->get_parameter("g2o_file", filename) && !filename.empty()) {
      // Load from single g2o file
      loadFromG2O(filename);
    } else {
      // Load num_robots measurements.csv
      loadFromMeasurements();
    }

    RCLCPP_INFO_STREAM(this->get_logger(), "DatasetPublisher: loaded data for "
                                               << num_robots << " robots.");

    for (size_t id = 0; id < (unsigned)num_robots; ++id) {
      string service_name = "/" + robotNames.at(id) +
                            "/distributed_loop_closure/request_pose_graph";
      auto server =
          this->create_service<pose_graph_tools_msgs::srv::PoseGraphQuery>(
              service_name,
              std::bind(&DatasetPublisher::queryPoseGraphCallback, this,
                        std::placeholders::_1, std::placeholders::_2));
      poseGraphServers.push_back(server);
    }
  }

  ~DatasetPublisher() = default;

private:
  int num_robots;
  vector<pose_graph_tools_msgs::msg::PoseGraph> poseGraphs;
  vector<rclcpp::Service<pose_graph_tools_msgs::srv::PoseGraphQuery>::SharedPtr>
      poseGraphServers;
  std::map<unsigned, std::string> robotNames;
  void queryPoseGraphCallback(
      const std::shared_ptr<pose_graph_tools_msgs::srv::PoseGraphQuery::Request>
          request,
      std::shared_ptr<pose_graph_tools_msgs::srv::PoseGraphQuery::Response>
          response) {
    if (request->robot_id >= poseGraphs.size()) {
      RCLCPP_ERROR(this->get_logger(),
                   "DatasetPublisher: requested robot does not exist!");
      return;
    }
    RCLCPP_INFO(this->get_logger(), "Received request from robot %i.",
                request->robot_id);
    response->pose_graph = poseGraphs[request->robot_id];
  }

  /**
   * @brief Initialize from a single dataset in g2o format
   * @param filename
   */
  void loadFromG2O(const std::string &filename) {
    size_t num_poses;
    vector<RelativeSEMeasurement> dataset = read_g2o_file(filename, num_poses);
    RCLCPP_INFO_STREAM(this->get_logger(), "Loaded dataset "
                                               << filename << " with "
                                               << num_poses << " total poses.");
    unsigned int n = num_poses;
    unsigned int num_poses_per_robot = n / num_robots;
    if (num_poses_per_robot <= 0) {
      RCLCPP_ERROR_STREAM(
          this->get_logger(),
          "Number of robots must be smaller than total number of poses!");
    }

    RCLCPP_INFO_STREAM(
        this->get_logger(),
        "Creating mapping from global pose index to local pose index...");
    map<unsigned, PoseID> PoseMap;
    for (unsigned robot = 0; robot < (unsigned)num_robots; ++robot) {
      unsigned startIdx = robot * num_poses_per_robot;
      unsigned endIdx = (robot + 1) * num_poses_per_robot; // non-inclusive
      if (robot == (unsigned)num_robots - 1)
        endIdx = n;
      for (unsigned idx = startIdx; idx < endIdx; ++idx) {
        unsigned localIdx = idx - startIdx; // this is the local ID of this pose
        PoseID pose(robot, localIdx);
        PoseMap[idx] = pose;
      }
    }

    vector<vector<RelativeSEMeasurement>> odometry(num_robots);
    vector<vector<RelativeSEMeasurement>> private_loop_closures(num_robots);
    vector<vector<RelativeSEMeasurement>> shared_loop_closure(num_robots);
    for (size_t k = 0; k < dataset.size(); ++k) {
      RelativeSEMeasurement mIn = dataset[k];
      PoseID src = PoseMap[mIn.p1];
      PoseID dst = PoseMap[mIn.p2];

      unsigned srcRobot = src.robot_id;
      unsigned srcIdx = src.frame_id;
      unsigned dstRobot = dst.robot_id;
      unsigned dstIdx = dst.frame_id;

      RelativeSEMeasurement m(srcRobot, dstRobot, srcIdx, dstIdx, mIn.R, mIn.t,
                              mIn.kappa, mIn.tau);

      if (srcRobot == dstRobot) {
        // private measurement
        if (srcIdx + 1 == dstIdx) {
          // Odometry
          odometry[srcRobot].push_back(m);
        } else {
          // private loop closure
          private_loop_closures[srcRobot].push_back(m);
        }
      } else {
        // shared measurement
        shared_loop_closure[srcRobot].push_back(m);
        // shared_loop_closure[dstRobot].push_back(m);
      }
    }

    for (size_t robot = 0; robot < (unsigned)num_robots; ++robot) {
      pose_graph_tools_msgs::msg::PoseGraph pose_graph;
      // Add odometry factors
      for (size_t k = 0; k < odometry[robot].size(); ++k) {
        pose_graph_tools_msgs::msg::PoseGraphEdge edge =
            dpgo_ros::RelativeMeasurementToMsg(odometry[robot][k]);
        pose_graph.edges.push_back(edge);
      }
      // Add private loop closures
      for (size_t k = 0; k < private_loop_closures[robot].size(); ++k) {
        pose_graph_tools_msgs::msg::PoseGraphEdge edge =
            dpgo_ros::RelativeMeasurementToMsg(private_loop_closures[robot][k]);
        pose_graph.edges.push_back(edge);
      }
      // Add shared loop closures
      for (size_t k = 0; k < shared_loop_closure[robot].size(); ++k) {
        pose_graph_tools_msgs::msg::PoseGraphEdge edge =
            dpgo_ros::RelativeMeasurementToMsg(shared_loop_closure[robot][k]);
        pose_graph.edges.push_back(edge);
      }
      poseGraphs.push_back(pose_graph);
    }
  }

  void loadFromMeasurements() {
    for (size_t robot_id = 0; robot_id < (unsigned)num_robots; ++robot_id) {
      pose_graph_tools_msgs::msg::PoseGraph pose_graph;
      std::string measurement_file;
      this->declare_parameter("robot" + std::to_string(robot_id) +
                                  "_measurements",
                              std::string(""));
      if (!this->get_parameter("robot" + std::to_string(robot_id) +
                                   "_measurements",
                               measurement_file) ||
          measurement_file.empty()) {
        RCLCPP_ERROR(this->get_logger(),
                     "No measurement file specified for robot %zu!", robot_id);
      }
      std::vector<RelativeSEMeasurement> measurements =
          PGOLogger::loadMeasurements(measurement_file, false);
      for (const auto &m : measurements) {
        pose_graph_tools_msgs::msg::PoseGraphEdge edge =
            dpgo_ros::RelativeMeasurementToMsg(m);
        pose_graph.edges.push_back(edge);
      }
      poseGraphs.push_back(pose_graph);
    }
  }
};

int main(int argc, char **argv) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<DatasetPublisher>();
  rclcpp::spin(node);
  rclcpp::shutdown();

  return 0;
}