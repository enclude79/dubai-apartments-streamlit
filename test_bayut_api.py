import http.client

API_HOST = "bayut.p.rapidapi.com"
API_KEY = "86b3cfbc80msh3cd99bad2e7126dp18c722jsnc5b7ca0b0d3d"

headers = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}

def test_keywords_list():
    print("\n=== Тест запроса /keywords/list ===")
    conn = http.client.HTTPSConnection(API_HOST)
    conn.request("GET", "/keywords/list?facetQuery=air&lang=en", headers=headers)
    res = conn.getresponse()
    print(f"Статус: {res.status} {res.reason}")
    data = res.read()
    print("Ответ:", data.decode("utf-8")[:500], "...\n")
    conn.close()

def test_properties_list():
    print("\n=== Тест запроса /properties/list ===")
    conn = http.client.HTTPSConnection(API_HOST)
    # Пример простого запроса с минимальным набором параметров
    url = "/properties/list?locationExternalIDs=5002&purpose=for-sale&hitsPerPage=2&page=1"
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    print(f"Статус: {res.status} {res.reason}")
    data = res.read()
    print("Ответ:", data.decode("utf-8")[:500], "...\n")
    conn.close()

if __name__ == "__main__":
    test_keywords_list()
    test_properties_list() 