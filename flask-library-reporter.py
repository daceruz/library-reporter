# Import libraries
import os
from datetime import datetime, timedelta
import asyncio
import time
import base64
import json
import requests
import pandas as pd
import aiohttp
from dotenv import load_dotenv # run: pip install python-dotenv
from flask import Flask, render_template,  jsonify, session, request, url_for, redirect, send_from_directory
from markupsafe import escape

# Define constants
BASE_URL = 'https://bookstack.library.com/api' # THIS IS AN EXAMPLE
MAX_ROWS_PER_FETCH = 500
PROGRESS_BAR_MAX = 100

# Load environment variables depending on if script is running locally or via a server (platform.sh).
local = False
try:
    variables = json.loads(base64.b64decode(os.getenv('PLATFORM_VARIABLES')).decode('utf-8'))
except TypeError:
    local = True
    load_dotenv()

def get_env(name):
    if local:
        return os.getenv(name)
    else:
        return variables[name]
    
# Defining username and password constants
USER_NAME = get_env('USER_NAME')
PASSWORD = get_env('PASSWORD')

# Key Authorization Setup
TOKEN_ID = get_env('TOKEN_ID')
TOKEN_SECRET = get_env('TOKEN_SECRET')
HEADERS = {
    'Authorization': f'Token {TOKEN_ID}:{TOKEN_SECRET}',
    'Content-Type': 'application/json'
}

# Global Variables
progress = {}
shelfid_slugname_dict = {}
shelfid_name_dict = {}
bookid_shelfid_dict = {}
bookid_slugname_dict = {}
bookid_name_dict = {}
chapterid_slugname_dict = {}
chapterid_name_dict = {}
chapterid_bookid_dict = {}
userid_owner_dict = {}
userid_email_dict = {}
shelfid_ownerid_dict = {}
bookid_ownerid_dict = {} 
chapterid_ownerid_dict = {}
pageid_name_dict = {}
pageid_slug_dict = {}
pageid_bookid_dict = {}

def api_request(ep, count=MAX_ROWS_PER_FETCH):
    # Sends a GET request to the specified API endpoint.

    response = requests.get(f'{BASE_URL}/{ep}', headers=HEADERS,  params={'count': count}, timeout=15) # Count is used to specify how many records will be returned in the response.

    # Checks whether it was a succesful response or not
    if response.status_code == 200:
        return response.json()
    else:
        print(f"\nFailed to fetch data from {BASE_URL}/{ep}.\n\nStatus code: {response.status_code}.\n\nError Message: {response.json()['error']['message']}\n")
        return

def run_setup():
    """
    Initializes and populates various dictionaries with data from API endpoints.

    This function sets up dictionaries to map:
        - User ID to name and email
        - Shelf ID to slug, name, and owner ID
        - Book ID to shelf ID, slug, name, and owner ID
        - Chapter ID to slug, name, book ID, and owner ID
        - Page ID to name, slug, and book ID
    """
     
    global shelfid_slugname_dict
    global shelfid_name_dict 
    global bookid_shelfid_dict
    global bookid_name_dict
    global bookid_slugname_dict
    global chapterid_slugname_dict
    global chapterid_name_dict
    global chapterid_bookid_dict
    global userid_owner_dict
    global userid_email_dict
    global shelfid_ownerid_dict
    global bookid_ownerid_dict
    global chapterid_ownerid_dict
    global chapterid_ownerid_dict
    global pageid_name_dict
    global pageid_slug_dict
    global pageid_bookid_dict 
    
    while True:
        # User Dictionaries
        setup_res = api_request('users', 1)
        setup_total = setup_res['total']
        initial_response = api_request('users', MAX_ROWS_PER_FETCH)
        user_data = {
            "data": initial_response['data'],
            "total": initial_response['total']
        }
        setup_total -= MAX_ROWS_PER_FETCH

        offset = MAX_ROWS_PER_FETCH
        while setup_total > 0:
            additional_response = api_request(f'users?offset={offset}', MAX_ROWS_PER_FETCH)
            user_data['data'] = user_data['data'] + additional_response['data']
            setup_total -= MAX_ROWS_PER_FETCH
            offset += MAX_ROWS_PER_FETCH

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in user_data:
            user_data = user_data['data'] 
        else:
            user_data = [user_data]  

        progress['p0'] = 0
        i_count = (PROGRESS_BAR_MAX / len(user_data))
        i = 0
        for user in user_data:
            userid_owner_dict[user['id']] = user['name']
            userid_email_dict[user['id']] = user['email']
            i += i_count
            progress['p0'] = i

        # Shelves Dictionaries
        setup_res = api_request('shelves', 1)
        setup_total = setup_res['total']
        initial_response = api_request('shelves', MAX_ROWS_PER_FETCH)
        shelves_data = {
            "data": initial_response['data'],
            "total": initial_response['total']
        }
        setup_total -= MAX_ROWS_PER_FETCH

        offset = MAX_ROWS_PER_FETCH
        while setup_total > 0:
            additional_response = api_request(f'shelves?offset={offset}', MAX_ROWS_PER_FETCH)
            shelves_data['data'] = shelves_data['data'] + additional_response['data']
            setup_total -= MAX_ROWS_PER_FETCH
            offset += MAX_ROWS_PER_FETCH

        if shelves_data:
            # Restructures json to return just a list rather than a list and the total number of records
            if 'data' in shelves_data:
                shelves_data = shelves_data['data'] 
            else:
                shelves_data = [shelves_data]  

            shelf_ids = [item['id'] for item in shelves_data]
            
            progress['p1'] = 0
            i_count = (PROGRESS_BAR_MAX / len(shelves_data))
            i = 0
            for shelf in shelves_data:
                shelfid_slugname_dict[shelf['id']] = shelf['slug']
                shelfid_name_dict[shelf['id']] = shelf['name']
                shelfid_ownerid_dict[shelf['id']] = shelf['owned_by']
                i += i_count
                progress['p1'] = i
        else:
            return

        # Book Dictionaries
        progress['p2'] = 0
        i_count = (PROGRESS_BAR_MAX / len(shelf_ids))
        i = 0
        for id in shelf_ids:
            shelf_data = api_request(f'shelves/{id}')
            for book in shelf_data['books']:
                if book['id'] in bookid_shelfid_dict:
                    bookid_shelfid_dict[book['id']].append(id)
                else:
                    bookid_shelfid_dict[book['id']] = [id]
            i += i_count
            progress['p2'] = i
        
        books_data = api_request('books', 300)


        setup_res = api_request('books', 1)
        setup_total = setup_res['total']
        initial_response = api_request('books', MAX_ROWS_PER_FETCH)
        books_data = {
            "data": initial_response['data'],
            "total": initial_response['total']
        }
        setup_total -= MAX_ROWS_PER_FETCH

        offset = MAX_ROWS_PER_FETCH
        while setup_total > 0:
            additional_response = api_request(f'books?offset={offset}', MAX_ROWS_PER_FETCH)
            books_data['data'] = books_data['data'] + additional_response['data']
            setup_total -= MAX_ROWS_PER_FETCH
            offset += MAX_ROWS_PER_FETCH

        if books_data:
            # Restructures json to return just a list rather than a list and the total number of records
            if 'data' in books_data:
                books_data = books_data['data']  
            else:
                books_data = [books_data] 

            progress['p3'] = 0
            i_count = (PROGRESS_BAR_MAX / len(books_data))
            i = 0
            for book in books_data:
                bookid_slugname_dict[book['id']] = book['slug']
                bookid_name_dict[book['id']] = book['name']
                bookid_ownerid_dict[book['id']] = book['owned_by']
                i += i_count
                progress['p3'] = i
            
        else:
            return
        

        # Chapter Dictionaries
        setup_res = api_request('chapters', 1)
        setup_total = setup_res['total']
        initial_response = api_request('chapters', MAX_ROWS_PER_FETCH)
        chapters_data = {
            "data": initial_response['data'],
            "total": initial_response['total']
        }
        setup_total -= MAX_ROWS_PER_FETCH

        offset = MAX_ROWS_PER_FETCH
        while setup_total > 0:
            additional_response = api_request(f'books?offset={offset}', MAX_ROWS_PER_FETCH)
            chapters_data['data'] = chapters_data['data'] + additional_response['data']
            setup_total -= MAX_ROWS_PER_FETCH
            offset += MAX_ROWS_PER_FETCH

        if chapters_data:
            # Restructures json to return just a list rather than a list and the total number of records
            if 'data' in chapters_data:
                chapters_data = chapters_data['data']  
            else:
                chapters_data = [chapters_data]  

            progress['p4'] = 0
            i_count = (PROGRESS_BAR_MAX / len(chapters_data))
            i = 0
            for chapter in chapters_data:
                chapterid_slugname_dict[chapter['id']] = chapter['slug']
                chapterid_name_dict[chapter['id']] = chapter['name']
                chapterid_ownerid_dict[chapter['id']] = chapter['owned_by']
                chapterid_bookid_dict[chapter['id']] = chapter['book_id']

                i += i_count
                progress['p4'] = i
        else:
            return
        
        # Pages Dictionaries
        setup_res = api_request('pages', 1)
        setup_total = setup_res['total']
        initial_response = api_request('pages', MAX_ROWS_PER_FETCH)
        pages_data = {
            "data": initial_response['data'],
            "total": initial_response['total']
        }
        setup_total -= MAX_ROWS_PER_FETCH

        offset = MAX_ROWS_PER_FETCH
        while setup_total > 0:
            additional_response = api_request(f'pages?offset={offset}', MAX_ROWS_PER_FETCH)
            pages_data['data'] = pages_data['data'] + additional_response['data']
            setup_total -= MAX_ROWS_PER_FETCH
            offset += MAX_ROWS_PER_FETCH

        if pages_data:
            # Restructures json to return just a list rather than a list and the total number of records
            if 'data' in pages_data:
                pages_data = pages_data['data']  
            else:
                pages_data = [pages_data]  

            progress['p5'] = 0
            i_count = (PROGRESS_BAR_MAX / len(pages_data))
            i = 0
            for page in pages_data:
                pageid_name_dict[page['id']] = page['name']
                pageid_slug_dict[page['id']] = page['slug']
                pageid_bookid_dict[page['id']] = page['book_id']

                i += i_count
                progress['p5'] = i
        else:
            return

        return

def run_reports():
    """
    Generates one excel file by retrieving all dataframes from each reporting function, 
    then seperating each by giving a unique sheet name.
    """
    pages_df = formatted_pages_report() 
    attachments_df = attachments_report()
    books_df = books_report()
    duplicate_books_df = duplicate_books_report()
    unshelved_books_df = unshelved_books_report()
    chapters_df = chapters_report()
    duplicate_pages_df = duplicate_pages_report()
    shelves_df = shelves_report()
    users_df = users_report()
    

    # Using ExcelWriter to write dataframes to separate sheets
    with pd.ExcelWriter(f"./reports/library-report.xlsx", engine='openpyxl') as writer:
        pages_df.to_excel(writer, sheet_name="Pages", index=False)
        attachments_df.to_excel(writer, sheet_name="Attachments", index=False)
        chapters_df.to_excel(writer, sheet_name="Chapters", index=False)
        books_df.to_excel(writer, sheet_name="Books", index=False)
        shelves_df.to_excel(writer, sheet_name="Shelves", index=False)
        users_df.to_excel(writer, sheet_name="Users", index=False)
        duplicate_books_df.to_excel(writer, sheet_name="Duplicate Books", index=False)
        unshelved_books_df.to_excel(writer, sheet_name="Unshelved Books", index=False)
        duplicate_pages_df.to_excel(writer, sheet_name="Duplicate Pages", index=False)

    time.sleep(2)
    progress.clear()
    
    return

def formatted_pages_report():
    """
    Generates a detailed report on pages, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all pages data from the API in batches.
    2. Collects tags for each page asynchronously.
    3. Constructs URLs and gathers detailed information (names, emails) for shelves, books, chapters, and pages.
    4. Formats tags and other attributes for readability.
    5. Transposes the collected data into a pandas DataFrame.
    6. Reorders and renames columns for clarity and drops unnecessary columns.
    """
    
    setup_res = api_request('pages', 1)
    setup_total = setup_res['total']
    initial_response = api_request('pages', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'pages?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    shelves_arr = []
    book_slug_arr = []
    book_name_arr = []
    chapter_slug_arr = []
    chapter_name_arr = []
    formatted_tags_arr = []
    book_owner_arr = []
    chapter_owner_arr = []
    page_owner_arr = []
    page_creator_arr = []
    page_updater_arr = []
    book_owneremail_arr = []
    chapter_owneremail_arr = []
    page_owneremail_arr = []
    page_creatoremail_arr = []
    page_updateremail_arr = []
    
    if data:
        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data]  

        # Setup of Variables
        page_ids = [item['id'] for item in data]
        pageid2tags ={}

        progress['p6'] = 0
        i_count = (PROGRESS_BAR_MAX / len(page_ids))
        i = 0

        for page_id in page_ids:
            page_data = api_request(f'pages/{page_id}')

            if page_data and 'tags' in page_data and page_data['tags']:
                formatted_string = ", ".join(tag['name'] for tag in page_data['tags'])
                pageid2tags[page_id] = formatted_string
            else:
                pageid2tags[page_id] = "No Tag(s)"

            i += i_count
            progress['p6'] = i

        progress['p7'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for page in data:
            # Setting up url columns
            shelves_name_url = ''
            shelfid_arr = bookid_shelfid_dict.get(page['book_id'])
            if shelfid_arr:
                for shelf_id in shelfid_arr:
                    shelves_name_url += f'{shelfid_name_dict.get(shelf_id)}: ' + f'https://bookstack.library.com/shelves/{shelfid_slugname_dict.get(shelf_id)}, '
                
                shelves_name_url = shelves_name_url[:-2] 
            else:
                shelves_name_url += 'No shelves found'
    
            shelves_arr.append(shelves_name_url)

            book_slug = bookid_slugname_dict.get(page['book_id'])
            book_name = bookid_name_dict.get(page['book_id'])
            if book_slug is not None and book_name is not None:
                book_slug_arr.append(f'=HYPERLINK("https://bookstack.library.com/books/{book_slug}")')
                book_name_arr.append(book_name)
            else:
                book_slug_arr.append("No Book")
                book_name_arr.append("No Book")

            chapter_slug = chapterid_slugname_dict.get(page['chapter_id'])
            chapter_name = chapterid_name_dict.get(page['chapter_id'])
            if chapter_slug is not None and chapter_name is not None:
                chapter_slug_arr.append(f'=HYPERLINK("https://bookstack.library.com/books/{bookid_slugname_dict.get(page['book_id'])}/chapter/{chapter_slug}")')
                chapter_name_arr.append(chapter_name)
            else:
                chapter_slug_arr.append("No Chapter")
                chapter_name_arr.append("No Chapter")

            # Formatting Tags Column
            formatted_tags_arr.append(pageid2tags[page['id']])

            # Reformats existing property "slug" into a link
            page['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{bookid_slugname_dict.get(page['book_id'])}/page/{page['slug']}")'

            # Creating owner name columns        
            owner_id = bookid_ownerid_dict.get(page['book_id'])
            book_owner = userid_owner_dict.get(owner_id)
            if book_owner is None: 
                book_owner = 'Book Owner Unknown'
            book_owner_arr.append(book_owner)

            owner_id = chapterid_ownerid_dict.get(page['chapter_id'])
            chapter_owner = userid_owner_dict.get(owner_id)
            if chapter_owner is None: 
                chapter_owner = 'Chapter Owner Unknown'
            chapter_owner_arr.append(chapter_owner)

            page_owner = userid_owner_dict.get(page['owned_by'])
            if page_owner is None: 
                page_owner = 'Page Owner Unknown'
            page_owner_arr.append(page_owner)

            page_creator = userid_owner_dict.get(page['created_by'])
            if page_creator is None: 
                page_creator = 'Page Creator Unknown'
            page_creator_arr.append(page_creator)

            page_updater = userid_owner_dict.get(page['updated_by'])
            if page_updater is None: 
                page_updater = 'Page Updater Unknown'
            page_updater_arr.append(page_updater)

            # Creating owner email columns
            owner_id = bookid_ownerid_dict.get(page['book_id'])
            book_owner_email = userid_email_dict.get(owner_id)
            if book_owner_email is None: 
                book_owner_email = 'Book Owner Email Unknown'
            book_owneremail_arr.append(book_owner_email)

            owner_id = chapterid_ownerid_dict.get(page['chapter_id'])
            chapter_owner_email = userid_email_dict.get(owner_id)
            if chapter_owner_email is None: 
                chapter_owner_email = 'Chapter Owner Email Unknown'
            chapter_owneremail_arr.append(chapter_owner_email)

            page_owner_email = userid_email_dict.get(page['owned_by'])
            if page_owner_email is None: 
                page_owner_email = 'Page Owner Email Unknown'
            page_owneremail_arr.append(page_owner_email)

            page_creator_email = userid_email_dict.get(page['created_by'])
            if page_creator_email is None: 
                page_creator_email = 'Page Creator Email Unknown'
            page_creatoremail_arr.append(page_creator_email)

            page_updater_email = userid_email_dict.get(page['updated_by'])
            if page_updater_email is None: 
                page_updater_email = 'Page Updater Email Unknown'
            page_updateremail_arr.append(page_updater_email)

            # Fixing the times to be more human-readable
            dt = datetime.strptime(page['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            page['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            dt = datetime.strptime(page['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            page['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            i += i_count
            progress['p7'] = i
    else:
        print("Formatted Pages Report Error")
        exit()
        
    df = pd.json_normalize(data)
    df['chapter_slug'] = chapter_slug_arr
    df['book_slug'] = book_slug_arr
    df['Chapter Name'] = chapter_name_arr
    df['Book Name'] = book_name_arr
    df['Shelves'] = shelves_arr
    df['tags'] = formatted_tags_arr
    df['page_owner'] = page_owner_arr
    df['Page Creator'] = page_creator_arr
    df['Page Updater'] = page_updater_arr
    df['chapter_owner'] = chapter_owner_arr
    df['book_owner'] = book_owner_arr
    df['Page Owner Email'] = page_owneremail_arr
    df['Page Creator Email'] = page_creatoremail_arr
    df['Page Updater Email'] = page_updateremail_arr
    df['Chapter Owner Email'] = chapter_owneremail_arr
    df['Book Owner Email'] = book_owneremail_arr

    
    # Restructuring/renaming of the dataframe
    reorder = ['name', 'slug', 'page_owner', 'Page Owner Email', 'Page Creator', 'Page Creator Email', 'Page Updater', 'Page Updater Email','draft', 'created_at', 'updated_at', 'tags', 'Chapter Name', 'chapter_slug', 'chapter_owner', 'Chapter Owner Email', 'Book Name', 'book_slug', 'book_owner', 'Book Owner Email', 'Shelves', 'id', 'book_id', 'chapter_id', 'template', 'priority', 'owned_by', 'created_by', 'updated_by', 'revision_count', 'editor']
    df = df[reorder]

    df = df.rename(columns={'name': 'Page Name', 'slug': 'Page URL', 'page_owner': 'Page Owner', 'draft': 'Draft Status', 'created_at': 'Created At', 'updated_at': 'Updated At', 'tags': 'Tags', 'chapter_slug': 'Chapter URL', 'chapter_owner': 'Chapter Owner', 'book_slug': 'Book URL', 'book_owner': 'Book Owner'})

    # Dropping Unneccesary Columns
    df = df.drop(['id', 'book_id', 'chapter_id', 'template', 'priority', 'owned_by', 'created_by', 'updated_by', 'revision_count', 'editor'], axis=1)

    return df
    
def attachments_report():
    """
    Generates a detailed report on attachments, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all attachments data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for attachments
    3. Formats tags and other attributes for readability.
    4. Transposes the collected data into a pandas DataFrame.
    5. Reorders and renames columns for clarity and drops unnecessary columns.
    """
    setup_res = api_request('attachments', 1)
    setup_total = setup_res['total']
    initial_response = api_request('attachments', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'attachments?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH
    
    if data:
        # Variable setup
        total = data['total']
        creator_arr, creator_email_arr, updater_arr, updater_email_arr, page_name_arr = [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data'] 
        else:
            data = [data] 

        progress['p8'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for atc in data:
            # Adding Names and Emails of Creators and Updaters
            creator = userid_owner_dict.get(atc['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)
            creator_email = userid_email_dict.get(atc['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)
            updater = userid_owner_dict.get(atc['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            updater_email = userid_email_dict.get(atc['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(atc['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            atc['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(atc['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            atc['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Names
            page_name = pageid_name_dict.get(atc['uploaded_to'])
            if page_name:
                page_name_arr.append(page_name)
            else:
                page_name_arr.append("No Page Found")

            # Matching Pages to Ids
            page_slug = pageid_slug_dict.get(atc['uploaded_to'])
            book_id = pageid_bookid_dict.get(atc['uploaded_to'])
            if page_slug and book_id:
                atc['uploaded_to'] = f'=HYPERLINK("https://bookstack.library.com/books/{bookid_slugname_dict.get(book_id)}/page/{page_slug}")'
            else:
                atc['uploaded_to'] = "No Page Found"

            # Formatting external true and false to yes and no
            if atc['external'] == True:
                atc['external'] = 'Yes'
            elif atc['external'] == False:
                atc['external'] = 'No'

            i += i_count
            progress['p8'] = i

        # Creation of the dataframe
        df = pd.json_normalize(data)
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        df['Page Name'] = page_name_arr
        
        # Dropping Unneccesary Columns
        df = df.drop(['id', 'order', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'extension', 'Page Name', 'uploaded_to', 'external', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Attachment Name', 'extension': 'Extension Type', 'uploaded_to': 'Page URL', 'external': 'Is The Attachment A Link?', 'created_at': 'Created At', 'updated_at': 'Updated At'})
        
        return df 
    else:
        print("Attachments Report Failed")
        exit()

def books_report():
    """
    Generates a detailed report on books, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all books data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for books and shelves
    3. Formats tags and other attributes for readability.
    4. Transposes the collected data into a pandas DataFrame.
    5. Reorders and renames columns for clarity and drops unnecessary columns.
    """

    setup_res = api_request('books', 1)
    setup_total = setup_res['total']
    initial_response = api_request('books', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'books?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr, shelves_arr = [], [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data] 

        progress['p9'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for book in data:
            # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(book['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(book['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(book['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(book['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(book['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)

            updater_email = userid_email_dict.get(book['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(book['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(book['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Ids
            book['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{book['slug']}")' # May be wrong

            # Setup for Shelves Column
            shelves_name_url = ''
            shelfid_arr = bookid_shelfid_dict.get(book['id'])
            if shelfid_arr:
                for shelf_id in shelfid_arr:
                    shelves_name_url += f'{shelfid_name_dict.get(shelf_id)}: ' + f'https://bookstack.library.com/shelves/{shelfid_slugname_dict.get(shelf_id)}, '
                
                shelves_name_url = shelves_name_url[:-2] 
            else:
                shelves_name_url += 'No shelves found'
    
            shelves_arr.append(shelves_name_url)

            # Fixing Descriptions:
            if book['description'] == '':
                book['description'] = "No Description"
            i += i_count
            progress['p9'] = i

        # Creation of the dataframe
        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        df['Shelves'] = shelves_arr
        
        # Dropping Unneccesary Columns
        df = df.drop(['id', 'owned_by', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'description', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at', 'Shelves']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Book Name', 'slug': 'Book URL', 'description': 'Description', 'created_at': 'Created At', 'updated_at': 'Updated At'})

        return df
    else:
        print('Books Report Failed')
        exit()

def duplicate_books_report():
    """
    Generates a detailed report on duplicate books, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all books data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for books and shelves
    3. Formats tags and other attributes for readability.
    4. Filters dataframe to only show duplicate items.
    5. Sorts dataframe by name for readability.
    6. Transposes the collected data into a pandas DataFrame.
    7. Reorders and renames columns for clarity and drops unnecessary columns.
    """

    setup_res = api_request('books', 1)
    setup_total = setup_res['total']
    initial_response = api_request('books', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'books?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr, shelves_arr = [], [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data]  
        
        # Creating the owner creator and updater arrs to add to the end of the dataframe
        progress['p10'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for book in data:
            # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(book['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(book['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(book['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(book['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(book['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            
            updater_email = userid_email_dict.get(book['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(book['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(book['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Ids
            book['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{book['slug']}")' # May be wrong

            # Setup for Shelves Column
            shelves_name_url = ''
            shelfid_arr = bookid_shelfid_dict.get(book['id'])
            if shelfid_arr:
                for shelf_id in shelfid_arr:
                    shelves_name_url += f'{shelfid_name_dict.get(shelf_id)}: ' + f'https://bookstack.library.com/shelves/{shelfid_slugname_dict.get(shelf_id)}, '
                
                shelves_name_url = shelves_name_url[:-2] 
            else:
                shelves_name_url += 'No shelves found'
    
            shelves_arr.append(shelves_name_url)

            # Fixing Descriptions:
            if book['description'] == '':
                book['description'] = "No Description"
            i += i_count
            progress['p10'] = i

        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        df['Shelves'] = shelves_arr
        df = df[df.duplicated('name', keep=False)]
        df = df.sort_values(by='name')

        # Dropping Unneccesary Columns
        df = df.drop(['id', 'owned_by', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'description', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at', 'Shelves']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Book Name', 'slug': 'Book URL', 'description': 'Description', 'created_at': 'Created At', 'updated_at': 'Updated At'})

        return df
    else:
        print("Duplicate Books Report Failed")
        exit()

def unshelved_books_report():
    """
    Generates a detailed report on unshelved books, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all books data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for books and shelves
    3. Formats tags and other attributes for readability.
    4. Iterates through list of all shelves, then iterates through each book on each shelf.
    5. If a book is found on a shelf, it is then removed from the list of books.
    6. Transposes the collected data into a pandas DataFrame.
    7. Reorders and renames columns for clarity and drops unnecessary columns.
    """
 
    setup_res = api_request('books', 1)
    setup_total = setup_res['total']
    initial_response = api_request('books', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'books?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr, shelves_arr = [], [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data]  

        # Creating the owner creator and updater arrs to add to the end of the dataframe
        for book in data:
            # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(book['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(book['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(book['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(book['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(book['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            
            updater_email = userid_email_dict.get(book['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(book['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(book['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            book['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Ids
            book['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{book['slug']}")' # May be wrong

            # Setup for Shelves Column
            shelves_name_url = ''
            shelfid_arr = bookid_shelfid_dict.get(book['id'])
            if shelfid_arr:
                for shelf_id in shelfid_arr:
                    shelves_name_url += f'{shelfid_name_dict.get(shelf_id)}: ' + f'https://bookstack.library.com/shelves/{shelfid_slugname_dict.get(shelf_id)}, '
                
                shelves_name_url = shelves_name_url[:-2] 
            else:
                shelves_name_url += 'No shelves found'
    
            shelves_arr.append(shelves_name_url)

            # Fixing Descriptions:
            if book['description'] == '':
                book['description'] = "No Description"

        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
    else:
        return

    # Sending request for all shelves
    data = api_request('shelves', 300)

    if data:
        if 'data' in data:
            data = data['data']  # In case of listing shelves
        else:
            data = [data]  # In case of a single shelf

        shelf_ids = [item['id'] for item in data]

        progress['p11'] = 0
        i_count = (PROGRESS_BAR_MAX / len(shelf_ids))
        i = 0
        for id in shelf_ids:
            # Sending request for specific shelf
            data = api_request(f'shelves/{id}', 300)
            shelf_books = data['books']
            book_ids = [item['id'] for item in shelf_books]
            for book_id in book_ids:
                df = df[df['id'] != book_id]
            
            i += i_count
            progress['p11'] = i
        
        # Dropping Unneccesary Columns
        df = df.drop(['id', 'owned_by', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'description', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Book Name', 'slug': 'Book URL', 'description': 'Description', 'created_at': 'Created At', 'updated_at': 'Updated At'})

        return df
    else:
        print("Unshelved Books Report Failed")
        exit()

def chapters_report():
    """
    Generates a detailed report on chapters, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all chapters data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for chapters, books and shelves
    3. Formats tags and other attributes for readability.
    4. Transposes the collected data into a pandas DataFrame.
    5. Reorders and renames columns for clarity and drops unnecessary columns.
    """
    
    setup_res = api_request('chapters', 1)
    setup_total = setup_res['total']
    initial_response = api_request('chapters', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'chapters?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable Setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr, book_name_arr = [], [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data] 

        progress['p12'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for chapter in data:
            # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(chapter['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(chapter['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(chapter['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(chapter['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(chapter['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            
            updater_email = userid_email_dict.get(chapter['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(chapter['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            chapter['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(chapter['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            chapter['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Fixing Chapter URL
            chapter['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{chapter['book_slug']}/chapter/{chapter['slug']}")' # May be wrong

            # Formatting Book name Column
            curr_bookname = bookid_name_dict.get(chapter['book_id'])
            book_name_arr.append(curr_bookname)

            # Fixing Book URL
            chapter['book_slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{chapter['book_slug']}/")' # May be wrong

            # Fixing Descriptions:
            if chapter['description'] == '':
                chapter['description'] = "No Description"

            i += i_count
            progress['p12'] = i

        # Creation of the dataframe
        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        df['Book Name'] = book_name_arr
        
        # Dropping Unneccesary Columns
        df = df.drop(['id', 'priority', 'book_id', 'owned_by', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'description', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at', 'Book Name', 'book_slug']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Chapter Name', 'slug': 'Chapter URL', 'description': 'Description', 'created_at': 'Created At', 'updated_at': 'Updated At', 'book_slug': 'Book URL'})

        return df
    else:
        print("Chapters Report Failed")
        exit()

def duplicate_pages_report():
    """
    Generates a detailed report on duplicate pages, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all pages data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for pages, chapters, books and shelves
    3. Formats tags and other attributes for readability.
    4. Filters dataframe to only show duplicate items.
    5. Sorts dataframe by name for readability.
    6. Transposes the collected data into a pandas DataFrame.
    7. Reorders and renames columns for clarity and drops unnecessary columns.
    """

    setup_res = api_request('pages', 1)
    setup_total = setup_res['total']
    initial_response = api_request('pages', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'pages?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr, book_name_arr = [], [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']  
        else:
            data = [data]  

        progress['p13'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for page in data:
           # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(page['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(page['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(page['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(page['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(page['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            
            updater_email = userid_email_dict.get(page['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(page['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            page['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(page['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            page['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Ids
            page['slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{page['book_slug']}/page/{page['slug']}")' 
            page['book_slug'] = f'=HYPERLINK("https://bookstack.library.com/books/{page['book_slug']}")'

            # Creating Book Name Column
            book_name = bookid_name_dict.get(page['book_id'])
            book_name_arr.append(book_name)

            i += i_count
            progress['p13'] = i

        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        df['Book Name'] = book_name_arr
        df = df[df.duplicated('name', keep=False)]
        df = df.sort_values(by='name')

        # Dropping Unneccesary Columns
        df = df.drop(['id', 'book_id', 'chapter_id', 'draft', 'template', 'priority', 'owned_by', 'created_by', 'updated_by', 'editor'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at' , 'revision_count', 'Book Name', 'book_slug']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Page Name', 'slug': 'Page URL', 'created_at': 'Created At', 'updated_at': 'Updated At', 'revision_count': 'Revision Count', 'book_slug': 'Book URL'})
        
        return df
    else:
        print('Duplicate Pages Report Failed')
        exit()

def shelves_report():
    """
    Generates a detailed report on shelves, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all shelves data from the API in batches.
    2. Constructs URLs and gathers detailed information (names, emails) for shelves
    3. Formats tags and other attributes for readability.
    4. Transposes the collected data into a pandas DataFrame.
    5. Reorders and renames columns for clarity and drops unnecessary columns.
    """

    setup_res = api_request('shelves', 1)
    setup_total = setup_res['total']
    initial_response = api_request('shelves', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'shelves?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']
        owner_arr, owner_email_arr, creator_arr, creator_email_arr, updater_arr, updater_email_arr = [], [], [], [], [], []

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']
        else:
            data = [data]

        progress['p14'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for shelf in data:
            # Adding Names and Emails of Creators and Updaters
            owner = userid_owner_dict.get(shelf['owned_by'])
            if owner is None: 
                owner = 'No Owner Found'
            owner_arr.append(owner)

            owner_email = userid_email_dict.get(shelf['owned_by'])
            if owner_email is None:
                owner_email = 'No Owner Email Found'
            owner_email_arr.append(owner_email)

            creator = userid_owner_dict.get(shelf['created_by'])
            if creator is None:
                creator = "No Creator Found"
            creator_arr.append(creator)

            creator_email = userid_email_dict.get(shelf['created_by'])
            if creator_email is None:
                creator_email = "No Creator Email Found"
            creator_email_arr.append(creator_email)

            updater = userid_owner_dict.get(shelf['updated_by'])
            if updater is None:
                updater = 'No Updater Found'
            updater_arr.append(updater)
            
            updater_email = userid_email_dict.get(shelf['updated_by'])
            if updater_email is None:
                updater_email = 'No Updater Found'
            updater_email_arr.append(updater_email)

            # Formatting Times
            dt = datetime.strptime(shelf['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            shelf['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(shelf['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            shelf['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Matching Pages to Ids
            shelf['slug'] = f'=HYPERLINK("https://bookstack.library.com/shelves/{shelf['slug']}")' # May be wrong

            # Fixing Descriptions:
            if shelf['description'] == '':
                shelf['description'] = "No Description"

            i += i_count
            progress['p14'] = i
        # Creation of the dataframe
        df = pd.json_normalize(data)
        df['Owner'] = owner_arr
        df['Owner Email'] = owner_email_arr
        df['Creator'] = creator_arr
        df['Creator Email'] = creator_email_arr
        df['Updater'] = updater_arr
        df['Updater Email'] = updater_email_arr
        
        # Dropping Unneccesary Columns
        df = df.drop(['id', 'owned_by', 'created_by', 'updated_by'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'slug', 'description', 'Owner', 'Owner Email', 'Creator', 'Creator Email', 'created_at', 'Updater', 'Updater Email', 'updated_at']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Shelf Name', 'slug': 'Shelf URL', 'description': 'Description', 'created_at': 'Created At', 'updated_at': 'Updated At'})

        return df
    else:
        print('Shelves Report Failed')
        exit()

def users_report():
    """
    Generates a detailed report on users, fetching data from an API and formatting it into a pandas DataFrame.

    This function performs the following steps:
    1. Fetches all users data from the API in batches.
    2. Constructs URLs and gathers detailed information (Creation Date, Last Activity, ...) for users
    3. Formats tags and other attributes for readability.
    4. Transposes the collected data into a pandas DataFrame.
    5. Reorders and renames columns for clarity and drops unnecessary columns.
    """

    setup_res = api_request('users', 1)
    setup_total = setup_res['total']
    initial_response = api_request('users', MAX_ROWS_PER_FETCH)
    data = {
        "data": initial_response['data'],
        "total": initial_response['total']
    }
    setup_total -= MAX_ROWS_PER_FETCH

    offset = MAX_ROWS_PER_FETCH
    while setup_total > 0:
        additional_response = api_request(f'users?offset={offset}', MAX_ROWS_PER_FETCH)
        data['data'] = data['data'] + additional_response['data']
        setup_total -= MAX_ROWS_PER_FETCH
        offset += MAX_ROWS_PER_FETCH

    if data:
        # Variable setup
        total = data['total']

        # Restructures json to return just a list rather than a list and the total number of records
        if 'data' in data:
            data = data['data']
        else:
            data = [data]

        progress['p15'] = 0
        i_count = (PROGRESS_BAR_MAX / len(data))
        i = 0
        for user in data:
            # Formatting Times
            if user['last_activity_at'] is None:
                user['last_activity_at'] = 'Unknown'
            else:
                dt = datetime.strptime(user['last_activity_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
                user['last_activity_at'] =  dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(user['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            user['created_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            dt = datetime.strptime(user['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            user['updated_at'] = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Formatting Profile URLS
            user['profile_url'] = f'=HYPERLINK("{user['profile_url']}")'
            user['edit_url'] = f'=HYPERLINK("{user['edit_url']}")'
            user['avatar_url'] = f'=HYPERLINK("{user['avatar_url']}")'

            i += i_count
            progress['p15'] = i

        # Creation of the dataframe
        df = pd.json_normalize(data)

        # Dropping Unneccesary Columns
        df = df.drop(['id', 'external_auth_id', 'slug'], axis=1)

        # Restructuring of the dataframe
        reorder = ['name', 'email', 'created_at', 'updated_at', 'last_activity_at', 'profile_url', 'edit_url', 'avatar_url']
        df = df[reorder]

        # Renaming of the dataframe
        df = df.rename(columns={'name': 'Name', 'email': 'User Email', 'created_at': 'Created At', 'updated_at': 'Updated At', 'last_activity_at': 'Last Activity At', 'profile_url': "Profile URL", 'edit_url': 'Edit URL', 'avatar_url': 'Avatar URL'})

        return df
    else:
        print('Users Report Failed')
        exit()

# Flask Application
app = Flask(__name__)

app.secret_key = get_env('APP_SECRET_KEY')
app.permanent_session_lifetime = timedelta(minutes=5)

@app.route("/")
def index():
    if 'username' in session:
        progress.clear()
        return render_template('home.html')
    else:
        return redirect('/login')
    
@app.route("/login", methods=['GET', 'POST'])
def login():
    incorrect = request.args.get('incorrect', 'false') == 'true'
    
    if request.method == 'POST':
        if (request.form['username'] == USER_NAME and
            request.form['password'] == PASSWORD):
            session.permanent = True
            session['username'] = request.form['username']
            return redirect(url_for('index'))
        else:
            return redirect(url_for('login', incorrect='true'))

    return render_template('login.html', status=incorrect)

@app.route('/logout')
def logout():
    if 'username' in session:
        # remove the username from the session if it's there
        session.pop('username', None)
        return redirect("/")
    else:
        return redirect('/login')

@app.route('/progress')
def get_progress():
    if 'username' in session:
        return jsonify(progress)
    else:
        return redirect('/login')

@app.route('/startsetup', methods=['POST'])
def startSetup():
    if 'username' in session:
        run_setup()
        return jsonify({"message": "Reports setup initiated successfully."}), 200
    else:
        return redirect('/login')

@app.route('/startreports', methods=['POST'])
def startReports():
    if 'username' in session:
        run_reports()
        return jsonify({"message": "Reports initiated successfully."}), 200
    else:
        return redirect('/login')

@app.route('/download/<filename>')
def download_file(filename):
    if 'username' in session:
        now = datetime.now()
        formatted_date = now.strftime("%Y-%m-%d")
        return send_from_directory('./reports/', escape(filename), as_attachment=True, download_name=f'library-report-{formatted_date}.xlsx')
    else:
        return redirect('/login')
