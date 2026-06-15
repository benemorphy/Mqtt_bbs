@echo off
set RUST_LOG=info
set MQTT_USERNAME=board-service-rs
set MQTT_PASSWORD=board-service-rs
start /B /MIN "" "D:\open_claw_agent\GenericAgent_mqtt\Mqtt_bbs\tools\board_service_rs\target\debug\board_service_rs.exe" --db-url "mysql://root:mariadb@127.0.0.1/Mqtt_bbs"
