# Script PowerShell simple pour télécharger OpenFoodFacts CSV
# Utilisation: .\scripts\download_data_simple.ps1

Write-Host "Telechargement du CSV OpenFoodFacts..." -ForegroundColor Green
Write-Host ""

# Créer le dossier
New-Item -ItemType Directory -Force -Path data\raw | Out-Null

# URLs
$url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
$gzFile = "data\raw\en.openfoodfacts.org.products.csv.gz"
$csvFile = "data\raw\en.openfoodfacts.org.products.csv"

Write-Host "Source: $url" -ForegroundColor Cyan
Write-Host "Destination: $gzFile" -ForegroundColor Cyan
Write-Host ""
Write-Host "ATTENTION: Fichier tres volumineux (environ 1.5 GB)" -ForegroundColor Yellow
Write-Host "Le telechargement peut prendre 15-30 minutes..." -ForegroundColor Yellow
Write-Host ""

# Télécharger avec barre de progression
try {
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $url -OutFile $gzFile -UseBasicParsing
    Write-Host "OK Telechargement termine!" -ForegroundColor Green
    Write-Host ""
}
catch {
    Write-Host "ERREUR lors du telechargement: $_" -ForegroundColor Red
    exit 1
}

# Vérifier si le fichier existe
if (-Not (Test-Path $gzFile)) {
    Write-Host "ERREUR: Le fichier .gz n'a pas ete telecharge" -ForegroundColor Red
    exit 1
}

Write-Host "Decompression du fichier..." -ForegroundColor Cyan
Write-Host "Cela peut prendre quelques minutes..." -ForegroundColor Yellow
Write-Host ""

# Décompression simple avec 7-Zip (si disponible) ou méthode alternative
try {
    # Méthode 1: Essayer avec 7-Zip
    $7zipPath = "C:\Program Files\7-Zip\7z.exe"
    if (Test-Path $7zipPath) {
        & $7zipPath e $gzFile "-o$((Get-Item $gzFile).DirectoryName)" -y | Out-Null
        Write-Host "OK Decompression avec 7-Zip terminee!" -ForegroundColor Green
    }
    else {
        # Méthode 2: PowerShell natif (plus lent)
        Write-Host "7-Zip non trouve, utilisation methode PowerShell..." -ForegroundColor Yellow
        
        $inStream = [System.IO.File]::OpenRead($gzFile)
        $outStream = [System.IO.File]::Create($csvFile)
        $gzipStream = New-Object System.IO.Compression.GZipStream($inStream, [System.IO.Compression.CompressionMode]::Decompress)
        
        $buffer = New-Object byte[](4096)
        while ($true) {
            $read = $gzipStream.Read($buffer, 0, 4096)
            if ($read -le 0) { break }
            $outStream.Write($buffer, 0, $read)
        }
        
        $gzipStream.Close()
        $outStream.Close()
        $inStream.Close()
        
        Write-Host "OK Decompression terminee!" -ForegroundColor Green
    }
}
catch {
    Write-Host "ERREUR lors de la decompression: $_" -ForegroundColor Red
    Write-Host "Essayez de decompresser manuellement avec 7-Zip ou WinRAR" -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# Vérifier le fichier final
if (Test-Path $csvFile) {
    $size = (Get-Item $csvFile).Length / 1GB
    Write-Host "OK Fichier CSV cree: $csvFile" -ForegroundColor Green
    Write-Host "Taille: $([math]::Round($size, 2)) GB" -ForegroundColor Cyan
    Write-Host ""
    
    # Afficher les premières lignes
    Write-Host "Apercu du fichier (3 premieres lignes):" -ForegroundColor Cyan
    Get-Content $csvFile -Head 3
    
    Write-Host ""
    Write-Host "OK TERMINE! Vous pouvez maintenant lancer: python etl/main.py" -ForegroundColor Green
    
    # Supprimer le .gz pour libérer de l'espace
    Write-Host ""
    Write-Host "Suppression du fichier .gz pour liberer de l'espace..." -ForegroundColor Yellow
    Remove-Item $gzFile -Force
    Write-Host "OK Fichier .gz supprime" -ForegroundColor Green
}
else {
    Write-Host "ERREUR: Le fichier CSV n'a pas ete cree" -ForegroundColor Red
    Write-Host "Le fichier .gz est toujours dans: $gzFile" -ForegroundColor Yellow
    Write-Host "Essayez de le decompresser manuellement" -ForegroundColor Yellow
}