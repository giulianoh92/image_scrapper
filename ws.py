import mysql.connector
import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Google Custom Search API details
API_KEY = os.getenv('API_KEY')
SEARCH_ENGINE_ID = os.getenv('SEARCH_ENGINE_ID')
SEARCH_URL = 'https://www.googleapis.com/customsearch/v1'

# MySQL connection details
def get_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="admin",
            password="1234",
            database="tpi_db"
        )
        if conn.is_connected():
            print("Connected to MySQL successfully!")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Fetch products with NULL image_path
def fetch_products():
    conn = get_mysql_connection()
    if conn is None:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT product_id, name FROM Products")
        products = cursor.fetchall()
        return products
    except mysql.connector.Error as err:
        print(f"Error fetching products: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

# Scrape image URL for a given product name
def scrape_image_url(product_name, retries=5):
    try:
        query = f"{product_name} site:amazon.com OR site:ebay.com OR site:bestbuy.com OR site:mercadolibre.com"
        
        params = {
            'key': API_KEY,
            'cx': SEARCH_ENGINE_ID,
            'q': query,
            'searchType': 'image',
            'num': 1  # Return 1 result
        }
        response = requests.get(SEARCH_URL, params=params)
        
        if response.status_code == 200:
            results = response.json()
            items = results.get('items', [])
            if items:
                img_url = items[0].get('link')
                if img_url:
                    print(f"Image URL found: {img_url} for product: {product_name}")
                    return img_url
            else:
                print(f"No image results found for {product_name}")
        elif response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                wait_time = int(retry_after)
            else:
                wait_time = 60  # Default wait time if Retry-After header is not present

            if retries > 0:
                print(f"Rate limit exceeded. Retrying after {wait_time} seconds. Retries left: {retries}")
                time.sleep(wait_time)
                return scrape_image_url(product_name, retries - 1)
            else:
                print(f"Rate limit exceeded and no retries left for {product_name}.")
        else:
            print(f"Failed to fetch search results for {product_name}: Status {response.status_code}")
    except Exception as e:
        print(f"Error scraping image for {product_name}: {e}")
    return None

# Download the image and save it locally
def download_image(img_url, product_name):
    folder = '/home/giu/workspace/javaCode/TPI-PA-2/src/main/resources/static/images'
    if not os.path.exists(folder):
        os.makedirs(folder)

    img_filename = f"{product_name.replace(' ', '_')}.jpg"
    img_filepath = os.path.join(folder, img_filename)
    try:
        img_data = requests.get(img_url).content
        with open(img_filepath, 'wb') as handler:
            handler.write(img_data)
        return img_filename
    except Exception as e:
        print(f"Error downloading image for {product_name}: {e}")
        return None

# Update the MySQL database with the new image path
def update_image_path(product_id, image_filename):
    conn = get_mysql_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        update_query = "UPDATE Products SET image_path = %s WHERE product_id = %s"
        cursor.execute(update_query, (image_filename, product_id))
        conn.commit()
        print(f"Updated image path in database for product ID {product_id}")
    except mysql.connector.Error as err:
        print(f"Error updating image path for product ID {product_id}: {err}")
    finally:
        cursor.close()
        conn.close()

def main():
    products = fetch_products()
    if not products:
        print("No products found or error fetching products.")
        return
    
    for product_id, product_name in products:
        print(f"Processing: {product_name}")

        # Web scrape for an image URL
        img_url = scrape_image_url(product_name)
        if img_url:
            print(f"Found image URL: {img_url}")

            # Download the image
            img_filename = download_image(img_url, product_name)
            if img_filename:
                print(f"Image saved as: {img_filename}")

                # Update the image path in the database
                update_image_path(product_id, img_filename)
            else:
                print(f"Failed to save image for {product_name}")
        else:
            print(f"No image found for {product_name}")

        # Delay to avoid hitting API rate limits
        time.sleep(2)

if __name__ == "__main__":
    main()