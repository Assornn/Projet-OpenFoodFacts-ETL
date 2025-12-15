# Script pour créer un sample de données OpenFoodFacts
# Sans télécharger le fichier complet

Write-Host "Creation d'un echantillon de donnees OpenFoodFacts..." -ForegroundColor Green
Write-Host ""

# Créer le dossier
New-Item -ItemType Directory -Force -Path data\raw | Out-Null

$sampleFile = "data\raw\openfoodfacts_sample.csv"

# Créer un CSV sample avec des données réalistes
$csvContent = @"
code	product_name	product_name_fr	product_name_en	brands	categories	countries	energy-kcal_100g	energy-kj_100g	fat_100g	saturated-fat_100g	carbohydrates_100g	sugars_100g	fiber_100g	proteins_100g	salt_100g	sodium_100g	nutriscore_grade	nova_group	ecoscore_grade	last_modified_t
3017620422003	Nutella	Nutella	Nutella	Ferrero	Pates a tartiner,Pates a tartiner au chocolat	France,Italie	539	2255	30.9	10.6	57.5	56.3	0	6.3	0.107	0.042	e	4	d	1638360000
8076809513463	Penne Rigate	Penne Rigate	Penne Rigate	Barilla	Pates,Pates de ble dur	Italie,France	359	1521	1.5	0.3	70.9	3.5	3.5	13	0.013	0.005	a	1	a	1638360001
5449000000439	Coca-Cola	Coca-Cola	Coca-Cola	Coca-Cola	Boissons,Sodas	France,USA	42	180	0	0	10.6	10.6	0	0	0.01	0.004	e	4	d	1638360002
3017760000109	Kinder Bueno	Kinder Bueno	Kinder Bueno	Ferrero	Chocolats,Barres chocolatees	France,Italie	571	2387	37.3	16	50.5	42.5	1.9	9	0.154	0.061	e	4	d	1638360003
3228857000852	Comté AOP	Comte AOP	Comte AOP	President	Fromages,Fromages a pate pressee cuite	France	409	1695	33	22	0	0	0	27	1.5	0.6	c	1	b	1638360004
3560070462926	Yaourt Nature	Yaourt Nature	Natural Yogurt	Danone	Yaourts,Produits laitiers	France	61	257	3.2	2.1	4.5	4.5	0	3.6	0.13	0.051	b	1	a	1638360005
3250392409722	Pain de Mie	Pain de Mie	Sliced Bread	Harry's	Pains,Pains de mie	France	265	1117	3.5	0.5	48	5	3	8.5	1.2	0.47	c	4	c	1638360006
3017620425035	Nutella B-ready	Nutella B-ready	Nutella B-ready	Ferrero	Barres chocolatees,Gaufres	France,Italie	527	2206	29.5	10.8	57.1	40.2	2.7	6.8	0.3	0.12	e	4	d	1638360007
3560070724216	Activia Nature	Activia Nature	Activia Natural	Danone	Yaourts,Yaourts nature	France	73	308	2.9	1.9	7.2	7.2	1.5	4.4	0.14	0.055	b	1	a	1638360008
3760091722508	Quinoa Bio	Quinoa Bio	Organic Quinoa	Primeal	Cereales,Quinoa	France,Perou	368	1539	6.1	0.7	57	0	7	14.1	0.013	0.005	a	1	a	1638360009
3270190207924	Lu Petit Ecolier	Lu Petit Ecolier	Lu Petit Ecolier	LU	Biscuits,Biscuits au chocolat	France	488	2043	19.5	11.5	68	33	2.7	6.5	0.66	0.26	d	4	d	1638360010
7622210449283	Milka Lait	Milka Lait	Milka Milk Chocolate	Milka	Chocolats,Chocolats au lait	France,Suisse	530	2218	30	18	57	56	1	6.5	0.44	0.17	e	4	d	1638360011
3017620429743	Kinder Surprise	Kinder Surprise	Kinder Surprise	Ferrero	Chocolats,Oeufs en chocolat	France,Italie	571	2392	37.6	23.5	49.5	48.8	0	8.7	0.326	0.129	e	4	d	1638360012
20724696	Pommes Golden	Pommes Golden	Golden Apples		Fruits,Pommes	France	52	220	0.2	0	11.4	10.4	2.4	0.3	0.002	0.001	a	1	a	1638360013
3760074380121	Lentilles Vertes	Lentilles Vertes Bio	Green Lentils	Markal	Legumineuses,Lentilles	France	116	490	0.6	0.1	17	1.8	7.9	9	0.004	0.002	a	1	a	1638360014
3560071057237	Actimel	Actimel	Actimel	Danone	Yaourts a boire,Produits laitiers	France	74	313	1.5	1	12.7	12.5	0	2.9	0.1	0.04	c	1	b	1638360015
26010858	Bananes	Bananes	Bananas		Fruits,Bananes	France,Equateur	89	377	0.3	0.1	20	12	2.6	1.1	0.001	0.0004	a	1	a	1638360016
3270160638406	Pringles Original	Pringles Original	Pringles Original	Pringles	Chips,Snacks	France,USA	536	2243	35	3	50	2.5	2.5	6	1.5	0.59	e	4	d	1638360017
40111421	Tomates	Tomates	Tomatoes		Legumes,Tomates	France,Espagne	18	76	0.2	0	3.9	2.6	1.2	0.9	0.005	0.002	a	1	a	1638360018
3017620422010	Ferrero Rocher	Ferrero Rocher	Ferrero Rocher	Ferrero	Chocolats,Pralines	France,Italie	576	2410	42	14.1	42.7	39.9	4.1	8.5	0.153	0.06	e	4	d	1638360019
3560070734177	Danette Chocolat	Danette Chocolat	Danette Chocolate	Danone	Desserts,Cremes dessert	France	121	509	2.8	1.8	19.6	18.6	0.8	3.2	0.14	0.055	d	4	c	1638360020
3083680085489	Cassegrain Petit Pois	Petits Pois Extra Fins	Extra Fine Peas	Cassegrain	Legumes en conserve,Petits pois	France	72	306	0.5	0.1	10	3.5	5.3	5.4	0.6	0.24	a	3	b	1638360021
26010841	Carottes	Carottes	Carrots		Legumes,Carottes	France	41	174	0.2	0	7.6	4.7	2.8	0.9	0.069	0.027	a	1	a	1638360022
3256540005167	Camembert	Camembert de Normandie AOP	Camembert	President	Fromages,Camemberts	France	299	1241	24	15.5	0.5	0.5	0	20	1.5	0.59	d	1	b	1638360023
3564700016909	Haricots Verts	Haricots Verts Extra Fins	Green Beans	Bonduelle	Legumes en conserve,Haricots verts	France	31	132	0.1	0	5	1	3	1.9	0.6	0.24	a	3	b	1638360024
"@

# Écrire le fichier
$csvContent | Out-File -FilePath $sampleFile -Encoding UTF8

Write-Host "OK Fichier sample cree: $sampleFile" -ForegroundColor Green
Write-Host ""

# Statistiques
$lines = (Get-Content $sampleFile | Measure-Object -Line).Lines
Write-Host "Nombre de produits: $($lines - 1)" -ForegroundColor Cyan
Write-Host "Taille du fichier: $([math]::Round((Get-Item $sampleFile).Length / 1KB, 2)) KB" -ForegroundColor Cyan
Write-Host ""

Write-Host "Apercu du fichier:" -ForegroundColor Yellow
Get-Content $sampleFile -Head 3
Write-Host ""

Write-Host "Pour utiliser ce sample:" -ForegroundColor Yellow
Write-Host "1. Modifier config.local.yaml:" -ForegroundColor White
Write-Host "   raw_data_path: 'data/raw/openfoodfacts_sample.csv'" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. Lancer l'ETL:" -ForegroundColor White
Write-Host "   python etl/main.py" -ForegroundColor Cyan
Write-Host ""

Write-Host "OK TERMINE!" -ForegroundColor Green