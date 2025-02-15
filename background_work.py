import kagglehub

while True:
    # Download latest version
    path = kagglehub.dataset_download("bhanupratapbiswas/weather-data")
    
    # Moving dataset into current directory
    print(path)