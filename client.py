import requests


def print_request_and_response_info_for_POST_or_PUT(request):
    print("---------------------------------------------------------------")
    print("request.url: ", request.url)
    print("request.status_code: ", request.status_code)
    print("request.headers: ", request.headers)
    print("request.text: ", request.text)
    print("request.request.body: ", request.request.body)
    print("request.request.headers: ", request.request.headers)
    print("---------------------------------------------------------------")


def print_request_and_response_info_for_GET_or_DELETE(request):
    print("---------------------------------------------------------------")
    print("request.url: ", request.url)
    print("request.status_code: ", request.status_code)
    print("request.headers: ", request.headers)
    print("request.text: ", request.text)
    print("request.request.headers: ", request.request.headers)
    print("---------------------------------------------------------------")


if __name__ == "__main__":
    host_IP = "127.0.0.1"
    API_port_number = 9898
    dummy_user_ID = 78
    dict_for_dummy_JSON_with_rating = {"userID": 78, "movieID": 903, "rating": 4.0, "Action": 0, "Adventure": 0,
                                       "Animation": 0, "Children": 0, "Comedy": 0, "Crime": 0, "Documentary": 0,
                                       "Drama": 1, "Fantasy": 0, "Film_Noir": 0, "Horror": 0, "IMAX": 0, "Musical": 0,
                                       "Mystery": 1, "Romance": 1, "Sci_Fi": 0, "Short": 0, "Thriller": 1, "War": 0,
                                       "Western": 0}
    request = requests.post("http://" + host_IP + ":" + str(API_port_number) + "/rating",
                            json=dict_for_dummy_JSON_with_rating)
    print_request_and_response_info_for_POST_or_PUT(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/all-users")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get(
        "http://" + host_IP + ":" + str(API_port_number) + "/avg-genre-ratings/" + str(dummy_user_ID))
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get(
        "http://" + host_IP + ":" + str(API_port_number) + "/profile/" + str(dummy_user_ID))
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.delete("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/ratings")
    print_request_and_response_info_for_GET_or_DELETE(request)
