Set WshShell = CreateObject("WScript.Shell")
WshShell.Environment("PROCESS")("RUST_LOG") = "info"
WshShell.Environment("PROCESS")("MQTT_USERNAME") = "board-service-rs"
WshShell.Environment("PROCESS")("MQTT_PASSWORD") = "board-service-rs"
WshShell.Run "D:\open_claw_agent\GenericAgent_mqtt\Mqtt_bbs\tools\board_service_rs\target\debug\board_service_rs.exe --db-url mysql://root:mariadb@127.0.0.1/Mqtt_bbs", 0, False
