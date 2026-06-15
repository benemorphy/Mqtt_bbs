Set WshShell = CreateObject("WScript.Shell")
WshShell.Environment("PROCESS")("RUST_LOG") = "info"
WshShell.Environment("PROCESS")("MQTT_USERNAME") = "board-service-rs"
WshShell.Environment("PROCESS")("MQTT_PASSWORD") = "board-service-rs"
WshShell.Environment("PROCESS")("MARIADB_PASSWORD") = "mariadb"
WshShell.Run "D:\open_claw_agent\Beneh\Mqtt_bbs_server\tools\board_service_rs\target\release\board_service_rs.exe --db-url mysql://root:mariadb@127.0.0.1/mqtt_bbs --jwt-secret bbs-browser-dev-secret-change-in-production --log-format json", 0, False
