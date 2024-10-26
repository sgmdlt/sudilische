from courts.producer import produce_tasks

search_params = {
    "court_url": ["http://giaginsky.adg.sudrf.ru/modules.php?"],
    "instance": [1],
    "type": ["УК"],
    "entry_date_from": "01.01.2023",
    "entry_date_to": "31.03.2023",
}

def main():
    results = produce_tasks(search_params)
    print(results)

if __name__ == "__main__":
    main()
