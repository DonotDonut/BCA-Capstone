from methods.airline_routes import AirlineRoutes
from methods.airlines import Airlines
from methods.airports import Airports
from methods.operational_airlines import OperationalAirlines
from methods.database import Database
from methods.report import Report
from methods.aircraft import Aircraft
from methods.plot import Plot


db_parameters = {
    "database_username": "#",
    "database_password": "#",
    "database_host": "#",
    "database_port": #,
    "database_name": "#"
}

#Airlines.load_airlines_to_db("input data/airlines.dat.txt", db_parameters)
#AirlineRoutes.load_routes_to_db("input data/routes.dat.txt", db_parameters)
#Airports.load_airports_to_db("input data/airports.dat.txt", db_parameters)
#Aircraft.load_aircraft_to_db("input data/planes.dat.txt", db_parameters)
#OperationalAirlines.create_table(db_parameters)
#Database.print_table_length(db_parameters, "operational_airlines") # 1255 operational airlines 
#AirlineRoutes.map_asia_flags(db_parameters)
#AirlineRoutes.count_asia_routes(db_parameters)
# 17855 departues and desitinations are in asia
#Report.create_asia_report_table(db_parameters)
#Database.print_table_length(db_parameters, "asia_report") #206 airlines 
''' Plot.export_asia_report_flights_pie(
        db_parameters,
        output_png_path="output data/asia_report_flights_pie.png",
        top_n=10,
        also_export_excel=False,
        output_excel_path="output data/pie_chart_asia_report_flights.xlsx",
    )
'''
#Airports.add_columns(db_parameters)
Airports.calculate_flights_per_airport(db_parameters)