#!/usr/bin/env python3
"""
AMR Oil Pipeline Sensor Producer
Fetches data from sensor API and produces to Kafka topic
"""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from confluent_kafka import Producer


# AMR Sensor data from API response
AMR_SENSOR_DATA = {
    "sensorId": "AMR-009",
    "type": "amr_oil_pipeline",
    "meterSerial": "AMR-PIPE-2024-09",
    "pipelineId": "PIPE-AMR-01",
    "location": "Oil Pipeline Station I",
    "flowRate": 215.4,
    "flowRateUnit": "L/min",
    "flowDirection": "forward",
    "cumulativeFlow": 524381.2,
    "cumulativeFlowUnit": "m³",
    "grossVolume": 27450.5,
    "netVolume": 27100.8,
    "volumeUnit": "L",
    "inletPressure": 6.32,
    "outletPressure": 4.18,
    "differentialPressure": 2.14,
    "pressureUnit": "bar",
    "temperature": 45.3,
    "temperatureUnit": "°C",
    "viscosity": 32.5,
    "viscosityUnit": "cSt",
    "density": 865.2,
    "densityUnit": "kg/m³",
    "apiGravity": 32.1,
    "waterContent": 0.52,
    "waterContentUnit": "%",
    "sulfurContent": 0.085,
    "sulfurContentUnit": "%",
    "correctionFactor": 0.9982,
    "pumpSpeed": 1450,
    "pumpSpeedUnit": "RPM",
    "valveStatus": "open",
    "valveOpenPercent": 87.5,
    "leakDetected": False,
    "leakSensitivity": "high",
    "batteryLevel": 78.4,
    "batteryUnit": "%",
    "signalStrength": -62,
    "signalUnit": "dBm",
    "lastCalibration": "2025-12-01T08:00:00.000Z",
    "nextCalibrationDue": "2026-06-01T08:00:00.000Z",
    "status": "normal",
    "alarmCode": None
}


def build_amr_event(sensor_data: dict, simulate_variation: bool = True) -> dict:
    """Build AMR sensor event with optional simulated variations"""
    import random
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device_id": sensor_data["sensorId"],
        "device_type": sensor_data["type"],
        "meter_serial": sensor_data["meterSerial"],
        "pipeline_id": sensor_data["pipelineId"],
        "location": sensor_data["location"],
    }
    
    if simulate_variation:
        # Add small variations to simulate real sensor readings
        event["flow_rate"] = round(sensor_data["flowRate"] + random.uniform(-2.0, 2.0), 2)
        event["flow_rate_unit"] = sensor_data["flowRateUnit"]
        
        event["inlet_pressure"] = round(sensor_data["inletPressure"] + random.uniform(-0.1, 0.1), 2)
        event["outlet_pressure"] = round(sensor_data["outletPressure"] + random.uniform(-0.1, 0.1), 2)
        event["differential_pressure"] = round(sensor_data["differentialPressure"] + random.uniform(-0.05, 0.05), 2)
        event["pressure_unit"] = sensor_data["pressureUnit"]
        
        event["temperature"] = round(sensor_data["temperature"] + random.uniform(-1.0, 1.0), 2)
        event["temperature_unit"] = sensor_data["temperatureUnit"]
        
        event["viscosity"] = round(sensor_data["viscosity"] + random.uniform(-0.5, 0.5), 2)
        event["viscosity_unit"] = sensor_data["viscosityUnit"]
        
        event["density"] = round(sensor_data["density"] + random.uniform(-1.0, 1.0), 2)
        event["density_unit"] = sensor_data["densityUnit"]
        
        event["cumulative_flow"] = round(sensor_data["cumulativeFlow"] + random.uniform(0, 0.5), 2)
        event["cumulative_flow_unit"] = sensor_data["cumulativeFlowUnit"]
        
        event["gross_volume"] = round(sensor_data["grossVolume"] + random.uniform(-0.5, 0.5), 2)
        event["net_volume"] = round(sensor_data["netVolume"] + random.uniform(-0.5, 0.5), 2)
        event["volume_unit"] = sensor_data["volumeUnit"]
        
        event["water_content"] = round(sensor_data["waterContent"] + random.uniform(-0.01, 0.01), 3)
        event["water_content_unit"] = sensor_data["waterContentUnit"]
        
        event["sulfur_content"] = round(sensor_data["sulfurContent"] + random.uniform(-0.001, 0.001), 4)
        event["sulfur_content_unit"] = sensor_data["sulfurContentUnit"]
        
        event["correction_factor"] = round(sensor_data["correctionFactor"] + random.uniform(-0.0001, 0.0001), 4)
        
        event["pump_speed"] = int(sensor_data["pumpSpeed"] + random.randint(-5, 5))
        event["pump_speed_unit"] = sensor_data["pumpSpeedUnit"]
        
        event["valve_status"] = sensor_data["valveStatus"]
        event["valve_open_percent"] = round(sensor_data["valveOpenPercent"] + random.uniform(-1.0, 1.0), 1)
        
        # Randomly simulate leak detection occasionally
        event["leak_detected"] = random.random() < 0.02  # 2% chance
        event["leak_sensitivity"] = sensor_data["leakSensitivity"]
        
        event["battery_level"] = round(sensor_data["batteryLevel"] - random.uniform(0, 0.01), 2)
        event["battery_unit"] = sensor_data["batteryUnit"]
        
        event["signal_strength"] = int(sensor_data["signalStrength"] + random.randint(-3, 3))
        event["signal_unit"] = sensor_data["signalUnit"]
        
        # Determine status based on various factors
        if event["leak_detected"] or event["battery_level"] < 20:
            status = "error"
        elif event["inlet_pressure"] < 5.0 or event["temperature"] > 80:
            status = "warn"
        else:
            status = "normal"
        
    else:
        # Use exact values from API
        event["flow_rate"] = sensor_data["flowRate"]
        event["flow_rate_unit"] = sensor_data["flowRateUnit"]
        event["inlet_pressure"] = sensor_data["inletPressure"]
        event["outlet_pressure"] = sensor_data["outletPressure"]
        event["differential_pressure"] = sensor_data["differentialPressure"]
        event["pressure_unit"] = sensor_data["pressureUnit"]
        event["temperature"] = sensor_data["temperature"]
        event["temperature_unit"] = sensor_data["temperatureUnit"]
        event["viscosity"] = sensor_data["viscosity"]
        event["viscosity_unit"] = sensor_data["viscosityUnit"]
        event["density"] = sensor_data["density"]
        event["density_unit"] = sensor_data["densityUnit"]
        event["cumulative_flow"] = sensor_data["cumulativeFlow"]
        event["cumulative_flow_unit"] = sensor_data["cumulativeFlowUnit"]
        event["gross_volume"] = sensor_data["grossVolume"]
        event["net_volume"] = sensor_data["netVolume"]
        event["volume_unit"] = sensor_data["volumeUnit"]
        event["water_content"] = sensor_data["waterContent"]
        event["water_content_unit"] = sensor_data["waterContentUnit"]
        event["sulfur_content"] = sensor_data["sulfurContent"]
        event["sulfur_content_unit"] = sensor_data["sulfurContentUnit"]
        event["correction_factor"] = sensor_data["correctionFactor"]
        event["pump_speed"] = sensor_data["pumpSpeed"]
        event["pump_speed_unit"] = sensor_data["pumpSpeedUnit"]
        event["valve_status"] = sensor_data["valveStatus"]
        event["valve_open_percent"] = sensor_data["valveOpenPercent"]
        event["leak_detected"] = sensor_data["leakDetected"]
        event["leak_sensitivity"] = sensor_data["leakSensitivity"]
        event["battery_level"] = sensor_data["batteryLevel"]
        event["battery_unit"] = sensor_data["batteryUnit"]
        event["signal_strength"] = sensor_data["signalStrength"]
        event["signal_unit"] = sensor_data["signalUnit"]
        status = sensor_data["status"]
    
    event["status"] = status
    
    # Add anomaly score based on status
    if status == "error":
        event["anomaly_score"] = round(0.7 + (random.random() * 0.3 if simulate_variation else 0), 4)
    elif status == "warn":
        event["anomaly_score"] = round(0.4 + (random.random() * 0.3 if simulate_variation else 0), 4)
    else:
        event["anomaly_score"] = round(random.uniform(0.0, 0.3) if simulate_variation else 0.0, 4)
    
    return event


def fetch_sensor_data(api_url: str) -> Optional[dict]:
    """Fetch sensor data from API"""
    try:
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        return response.json().get("data")
    except Exception as e:
        print(f"Failed to fetch from API: {e}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="AMR Oil Pipeline Sensor Producer")
    parser.add_argument("--brokers", default="redpanda:9092", help="Kafka brokers")
    parser.add_argument("--topic", default="iot.telemetry.raw.v1", help="Kafka topic")
    parser.add_argument("--rate", type=int, default=1, help="events per second")
    parser.add_argument("--api-url", default="http://192.168.102.201:4040/api/v1/sensors/amr", 
                        help="Sensor API URL")
    parser.add_argument("--use-api", action="store_true", help="Fetch data from API instead of using default")
    parser.add_argument("--no-variation", action="store_true", help="Use exact values without simulation")
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.brokers,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "enable.idempotence": True,
            "retries": 20,
        }
    )

    # Get sensor data
    if args.use_api:
        sensor_data = fetch_sensor_data(args.api_url)
        if not sensor_data:
            print("Using default AMR sensor data")
            sensor_data = AMR_SENSOR_DATA
    else:
        sensor_data = AMR_SENSOR_DATA

    print(f"Producer started - sending {args.rate} events/sec to topic '{args.topic}'")
    print(f"Sensor: {sensor_data['sensorId']} - {sensor_data['location']}")
    print(f"API URL: {args.api_url}")
    print(f"Simulation: {'disabled' if args.no_variation else 'enabled'}")
    
    sent_total = 0
    
    while True:
        start = time.time()
        for _ in range(args.rate):
            event = build_amr_event(sensor_data, simulate_variation=not args.no_variation)
            producer.produce(
                args.topic,
                key=event["device_id"],
                value=json.dumps(event, separators=(",", ":")),
            )
        producer.poll(0)
        producer.flush(timeout=0.2)

        sent_total += args.rate
        elapsed = time.time() - start
        sleep_for = max(0.0, 1.0 - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)

        print(f"produced={sent_total} rate={args.rate}/s cycle={elapsed:.3f}s")


if __name__ == "__main__":
    main()
