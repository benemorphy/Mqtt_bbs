Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c cd /d D:\open_claw_agent\GenericAgent_mqtt\Mqtt_bbs\tools\board_service_rs && set RUST_LOG=info && cargo build 2>&1 > build3.log", 0, False
