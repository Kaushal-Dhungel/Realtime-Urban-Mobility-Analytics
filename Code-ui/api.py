from tiny_api.api import Tinyapi
from tiny_api.Responses import TemplateResponse
from tiny_api.Status import HTTP_200_OK

from connection import send_to_event_hub, generate_uber_ride_confirmation

app = Tinyapi()

@app.route('/', allowed_methods=['GET','POST'])
def template_view(request):
    return TemplateResponse(app=app,template_name='home.html', context = {'title':'Home'})

@app.route('/book', allowed_methods=['GET','POST'])
def book_ride(request):
    ride = generate_uber_ride_confirmation()
    result = send_to_event_hub(ride)
    print("Confirmation result is =====", result)
    return TemplateResponse(app=app,template_name='confirmation.html', context = {'title':'Book'})     

