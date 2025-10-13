cmake_minimum_required(VERSION 3.8)
project(dpgo_ros)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

SET(CMAKE_BUILD_TYPE Release)

find_package(DPGO REQUIRED)
find_package(glog REQUIRED)

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(rclpy REQUIRED)
find_package(std_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(visualization_msgs REQUIRED)
find_package(nav_msgs REQUIRED)
find_package(tf2_geometry_msgs REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(pose_graph_tools_msgs REQUIRED)
find_package(pose_graph_tools_ros REQUIRED)

# # Compile as C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -march=native -pthread -O3 -DPI=3.14159265358979323846")

# Generate interfaces
rosidl_generate_interfaces(${PROJECT_NAME}
  "msg/Status.msg"
  "msg/Command.msg"
  "msg/MatrixMsg.msg"
  "msg/PublicPoses.msg"
  "msg/RelativeMeasurementWeights.msg"
  "msg/RelativeMeasurementList.msg"
  "srv/QueryLiftingMatrix.srv"
  DEPENDENCIES std_msgs geometry_msgs pose_graph_tools_msgs
)

# Include directories
include_directories(include)

# Create library
add_library(${PROJECT_NAME}_lib
  src/PGOAgentROS.cpp
  src/utils.cpp
)

# Add dependencies to ensure message generation happens first
rosidl_get_typesupport_target(cpp_typesupport_target ${PROJECT_NAME} "rosidl_typesupport_cpp")
target_link_libraries(${PROJECT_NAME}_lib "${cpp_typesupport_target}")

ament_target_dependencies(${PROJECT_NAME}_lib
  rclcpp
  rclcpp_components
  std_msgs
  geometry_msgs
  sensor_msgs
  visualization_msgs
  nav_msgs
  tf2_geometry_msgs
  pose_graph_tools_msgs
  pose_graph_tools_ros
)

target_link_libraries(${PROJECT_NAME}_lib
  DPGO
  glog::glog
)

# Create executables
add_executable(${PROJECT_NAME}_node src/PGOAgentROSNode.cpp)
add_executable(${PROJECT_NAME}_dataset_publisher_node src/PGODatasetPublisherNode.cpp)

ament_target_dependencies(${PROJECT_NAME}_node
  rclcpp
  rclcpp_components
  std_msgs
  geometry_msgs
  sensor_msgs
  visualization_msgs
  nav_msgs
  tf2_geometry_msgs
  pose_graph_tools_msgs
  pose_graph_tools_ros
)

ament_target_dependencies(${PROJECT_NAME}_dataset_publisher_node
  rclcpp
  rclcpp_components
  std_msgs
  geometry_msgs
  sensor_msgs
  visualization_msgs
  nav_msgs
  tf2_geometry_msgs
  pose_graph_tools_msgs
  pose_graph_tools_ros
)

target_link_libraries(${PROJECT_NAME}_node
  ${PROJECT_NAME}_lib
)

target_link_libraries(${PROJECT_NAME}_dataset_publisher_node
  ${PROJECT_NAME}_lib
)

# Testing
if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  find_package(ament_cmake_gtest REQUIRED)

  # Add gtest
  ament_add_gtest(test_utils tests/testUtils.cpp)

  if(TARGET test_utils)
    target_link_libraries(test_utils ${PROJECT_NAME}_lib -ltbb)
    ament_target_dependencies(test_utils rclcpp)
  endif()

  # the following line skips the linter which checks for copyrights
  # comment the line when a copyright and license is added to all source files
  set(ament_cmake_copyright_FOUND TRUE)

  # the following line skips cpplint (only works in a git repo)
  # comment the line when this package is in a git repo and when
  # a copyright and license is added to all source files
  set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

# Install
install(TARGETS
  ${PROJECT_NAME}_lib
  ${PROJECT_NAME}_node
  ${PROJECT_NAME}_dataset_publisher_node
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION lib/${PROJECT_NAME}
)

install(DIRECTORY
  include/
  DESTINATION include/
)

install(DIRECTORY
  launch/
  DESTINATION share/${PROJECT_NAME}/launch/
)

# install the data folder
install(DIRECTORY
  data/
  DESTINATION share/${PROJECT_NAME}/data/
)

# Install params directory if it exists
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/params")
  install(DIRECTORY
    params/
    DESTINATION share/${PROJECT_NAME}/params/
  )
endif()

ament_export_include_directories(include)
ament_export_libraries(${PROJECT_NAME}_lib)
ament_export_dependencies(
  rclcpp
  std_msgs
  geometry_msgs
  sensor_msgs
  visualization_msgs
  rosidl_default_runtime
  pose_graph_tools_msgs
  pose_graph_tools_ros
)

ament_package()
