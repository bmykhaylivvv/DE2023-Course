import requests
import easyocr
import json
from datetime import datetime
from bs4 import BeautifulSoup
from openai import OpenAI
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def extract_domains_from_ocr_text_list(text_list):
    client = OpenAI(
        api_key=Variable.get("OPEN_AI_API_KEY"),
    )

    chat_completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": "Always return response in JSON and the following format: ['domain1, 'domain2']. Do not return duplicates",
            },
            {
                "role": "user",
                "content": "Always return response the following format: ['domain1, 'domain2']. \n\n Domains are expected to end with typical website endings like '.com', '.org', '.net', etc. \n\n Do not return duplicates \n\n If no domain is found, return an empty list.",
            },
            {
                "role": "user",
                "content": f"Extract existing website domain names which should end as websites end from this list of words and return of JSON list format\\n\\nIf no domain found, return empty list\\n\\n{text_list}",
            },
        ],
        temperature=1,
        max_tokens=512,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
    )
    try:
        parsed_response = json.loads(chat_completion.choices[0].message.content)

        if type(parsed_response) is list:
            return parsed_response

        if type(parsed_response) is dict:
            if "domains" in parsed_response:
                dict_domains = parsed_response["domains"]

                if type(dict_domains) is list:
                    return dict_domains
    except:
        return []

    return []


def scrape_image_urls(ti, url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            img_tags = soup.find_all("img")
            image_urls = [
                img.get("src") for img in img_tags if img.get("src") is not None
            ]

            ti.xcom_push("image_urls", image_urls)
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")

        ti.xcom_push("image_urls", [])


def perform_ocr(ti):
    image_urls = ti.xcom_pull(key="image_urls", task_ids="scrape_images")
    reader = easyocr.Reader(
        ["en"]
    )

    postgres_hook = PostgresHook(postgres_conn_id="postgres_homework_2")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    for url in image_urls:
        print("url: ", url)

        try:
            result = reader.readtext(url, detail=0)
            print("OCR Result: ", result)

            possible_domains = extract_domains_from_ocr_text_list(result)
            print("possible domains: ", possible_domains)

            for domain in possible_domains:
                cursor.execute(
                    "INSERT INTO DomainsToScrape (domain, scrapped) VALUES (%s, %s) ON CONFLICT (domain) DO NOTHING;",
                    (domain, False),
                )
            conn.commit()

        except Exception as e:
            print("Something went wrong. ERR: ", e)

    cursor.close()
    conn.close()


def get_domains_info():
    postgres_hook = PostgresHook(postgres_conn_id="postgres_homework_2")
    select_query = "SELECT domain FROM DomainsToScrape WHERE scrapped = False;"
    records = postgres_hook.get_records(sql=select_query)
    domains_to_scrape = [record[0] for record in records]
    client = OpenAI(api_key=Variable.get("OPEN_AI_API_KEY"))

    for domain in domains_to_scrape:
        try:
            chat_completion = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "user",
                        "content": f"Give me info about this domain: {domain}",
                    }
                ],
                temperature=1,
                max_tokens=512,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
            )
            info = chat_completion.choices[0].message.content.strip()

            insert_query = "INSERT INTO DomainsInfo (domain, info) VALUES (%s, %s);"
            postgres_hook.run(insert_query, parameters=(domain, info))

            update_query = "UPDATE DomainsToScrape SET scrapped = True WHERE domain = %s;"
            postgres_hook.run(update_query, parameters=(domain,))

        except Exception as e:
            print(f"An error occurred while processing {domain}: {e}")


with DAG(
    "DE_HW2_Data_Scrapping_Bohdan_Mykhailiv",
    schedule_interval="@daily",
    start_date=datetime(2023, 12, 11),
    catchup=False,
) as dag:
    create_domains_table = PostgresOperator(
        task_id="create_domains_table",
        postgres_conn_id="postgres_homework_2",
        sql="""
        CREATE TABLE IF NOT EXISTS DomainsToScrape (
            id  SERIAL PRIMARY KEY,
            domain VARCHAR(128) UNIQUE,
            scrapped BOOlEAN
        );
        """,
    )

    create_domains_info_table = PostgresOperator(
        task_id="create_domains_info_table",
        postgres_conn_id="postgres_homework_2",
        sql="""
        CREATE TABLE IF NOT EXISTS DomainsInfo (
            id  SERIAL PRIMARY KEY,
            domain VARCHAR(128),
            info TEXT
        );
        """,
    )

    scrape_images_task = PythonOperator(
        task_id="scrape_images",
        python_callable=scrape_image_urls,
        op_kwargs={
            "url": Variable.get("URL_TO_SCRAPE")
        },
        dag=dag,
    )

    ocr_task = PythonOperator(
        task_id="perform_ocr",
        python_callable=perform_ocr,
        dag=dag,
    )

    get_domains_info = PythonOperator(
        task_id="get_domains_info",
        python_callable=get_domains_info,
        dag=dag,
    )

    create_domains_table >> create_domains_info_table >> scrape_images_task >> ocr_task >> get_domains_info
