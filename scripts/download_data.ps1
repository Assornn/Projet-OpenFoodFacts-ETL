# Script PowerShell de téléchargement OpenFoodFacts CSV

Write-Host "📥 Téléchargement du CSV OpenFoodFacts..." -ForegroundColor Green

# Créer le dossier
New-Item -ItemType Directory -Force -Path data/raw | Out-Null

# URL source
$url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
$gzFile = "data/raw/en.openfoodfacts.org.products.csv.gz"
$csvFile = "data/raw/en.openfoodfacts.org.products.csv"

Write-Host "Téléchargement depuis: $url"
Write-Host "⚠️  Fichier volumineux (~1.5 GB), cela peut prendre du temps..."

# Télécharger
Invoke-WebRequest -Uri $url -OutFile $gzFile -UseBasicParsing

Write-Host "✓ Téléchargement terminé"
Write-Host "Décompression..."

# Décompresser avec .NET
Add-Type -AssemblyName System.IO.Compression.FileSystem
$gzStream = New-Object System.IO.FileStream($gzFile, [System.IO.FileMode]::Open)
$output = New-Object System.IO.FileStream($csvFile, [System.IO.FileMode]::Create)
$gzipStream = New-Object System.IO.Compression.GZipStream($gzStream, [System.IO.Compression.CompressionMode]::Decompress)

$buffer = New-Object byte[](1024)
while ($true) {
    $read = $gzipStream.Read($buffer, 0, 1024)
    if ($read -le 0) { break }
    $output.Write($buffer, 0, $read)
}

$gzipStream.Close()
$output.Close()
$gzStream.Close()

Write-Host "✓ Décompression terminée" -ForegroundColor Green
Write-Host "Fichier: $csvFile"

# Supprimer le .gz
Remove-Item $gzFile

Write-Host "✓ Terminé!" -ForegroundColor Green
