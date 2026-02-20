# Start aggregator and producer services and tail aggregator logs (PowerShell)
Write-Host "Starting aggregator and producer..."
docker-compose up -d --build aggregator producer

Write-Host "Tailing aggregator logs (press Ctrl+C to stop)"
docker-compose logs -f aggregator