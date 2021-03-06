from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.airstats
collection = db['flight']

def mainPrompt():
    print('Welcome! What would you like to do today? Here are your choices:'
          '\n1  - Find the number of flights coming from a specific state'
          '\n2  - Lists the months and the average departure delay experienced in those months'
          '\n3  - Find the average flight time from flights between certain airports'
          '\n4  - Find all airport codes in a certain state'
          '\n5  - Find the probability that a flight going to an airport will be cancelled'
          '\n6  - Find the average departure delay of a certain airport'
          '\n7  - Find the average arrival delay time for a certain flight carrier'
          '\n8  - Lists the months and the average taxi out time in a given airport'
          '\n9  - Find the average delay due to weather when leaving from a certain airport'
          '\n10 - Find the number of cancelled flights in a given month and year'
          '\n11 - Find the distance in miles between two airports'
          '\n12 - Find the average delay due to late aircraft given a specific carrier and year'
          '\n13 - Find the month and delay with the longest delay given a year'
          '\n14 - Finds the destination state that most flights go to given an origin state'
          '\n15 - List the month and the carrier with the most amount of flights cancelled in a given year')
    print()
    userValue = input('Enter your choice here: ')
    if userValue == '1':
        value = input('What state would you like to see? ')
        numOfFlightsFromState(value)
    elif userValue == '2':
        getAverageDepartureDelayPerMonth()
    elif userValue == '3':
        origin = input('Origin: ')
        destination = input('Destination: ')
        averageFlightTimeBetweenAirports(origin, destination)
    elif userValue == '4':
        state = input('What state would you like to see? ')
        getAirportCodesInState(state)
    elif userValue == '5':
        airport = input('What airport would you like to see? ')
        probabilityOfCancellation(airport)
    elif userValue == '6':
        airport = input('What airport would you like to see? ')
        averageDepartureDelayAtAirport(airport)
    elif userValue == '7':
        carrier = input('What carrier would you like to see? ')
        averageDelayTimePerFlightCarrier(carrier)
    elif userValue == '8':
        airport = input('What airport would you like to see? ')
        averageTaxiOutTimePerMonth(airport)
    elif userValue == '9':
        airport = input('What airport would you like to see? ')
        averageWeatherDelayWhenLeaving(airport)
    elif userValue == '10':
        month = input('Which month? ')
        year = input('Which year? ')
        findNumberOfCancelledFlights(month, year)
    elif userValue == '11':
        airport1 = input('From airport: ')
        airport2 = input('to airport: ')
        findDistanceBetweenAirports(airport1, airport2)
    elif userValue == '12':
        carrier = input('What carrier would you like to see? ')
        year = input('Which year? ')
        averageLateAircraftDelayFromCarrier(carrier, year)
    elif userValue == '13':
        year = input('Which year? ')
        findTheMonthWithTheLongestDelayGivenAYear(year)
    elif userValue == '14':
        state = input('What state would you like to see? ')
        findMostTraveledStateFromGivenState(state)
    elif userValue == '15':
        year = input('What year would you like to see? ')
        listMonthAndCarrierOfMostCancelledFlights(year)

# Find the number of flights coming from a specific state
def numOfFlightsFromState(state):
    print('The number of flights coming from {} is {}'.format(state, collection.find({'ORIGIN_STATE_NM': state}).count()))

# Lists the months and the average departure delay experienced in those months
def getAverageDepartureDelayPerMonth():
    results = collection.aggregate([{'$group': {'_id': "$MONTH", 'avgDelay': {'$avg': "$DEP_DELAY"}}}, {'$sort': {'_id' : 1}}])
    print('Month |  Delay in Minutes')
    print('---------------------------')
    for result in results:
        if result['_id'] is not None:
            print("%-6d|%9.2f" % (result['_id'], result['avgDelay']))

# Find the average flight time from flights between certain airports
def averageFlightTimeBetweenAirports(origin, destination):
    results = collection.aggregate([{'$match': {"ORIGIN": origin, "DEST": destination}}, {'$group': {'_id': "$ORIGIN", 'avgFlightTime': {'$avg': "$AIR_TIME"}}}])
    for result in results:
        print("The average time of flight from %s to %s is %.2f minutes" % (origin, destination, result['avgFlightTime']))

# Find all airport codes in a certain state
def getAirportCodesInState(state):
    results = collection.aggregate([{'$match': {"ORIGIN_STATE_NM": state}}, {'$group': {'_id': { 'origin' : "$ORIGIN", 'city': "$ORIGIN_CITY_NAME"}} }])
    print('Code | City, State')
    print('-------------------------')
    for result in results:
        print("%-5s| %s" % (result['_id']['origin'], result['_id']['city']))

# Find the probability that a flight going to a certain airport will be cancelled
def probabilityOfCancellation(airport):
    results = collection.aggregate([{'$match': {"DEST": airport}},{'$group':{'_id': "$DEST", 'chance_of_cancellation': {'$avg': "$CANCELLED"}}}])
    for result in results:
        print("The probability that a flight going to %s will be cancelled is %.4f or %2.2f percent" % (airport, result['chance_of_cancellation'], result['chance_of_cancellation'] * 100.0))

# Find the average departure delay of a certain airport
def averageDepartureDelayAtAirport(airport):
    results = collection.aggregate([{'$match': {"ORIGIN": airport}}, {'$group': {'_id': "$ORIGIN",'average_departure_delay': { '$avg': "$DEP_DELAY"}}}])
    for result in results:
        print("The average departure delay from %s is %.2f minutes" % (airport, result['average_departure_delay']))

# Find the average arrival delay time for a certain flight carrier
def averageDelayTimePerFlightCarrier(carrier):
    results = collection.aggregate([{'$match': {"CARRIER": carrier}},{'$group':{'_id': "$CARRIER",'average_arrival_delay': { '$avg': "$ARR_DELAY" }}}])
    for result in results:
        print("The average delay time of %s is %2.2f minutes" % (carrier, result['average_arrival_delay']))

# Lists the months and the average taxi out time in a given airport
def averageTaxiOutTimePerMonth(airport):
    results = collection.aggregate([{'$match': {"ORIGIN": airport}}, {'$group': {'_id': "$MONTH", 'avgTaxiOutTime': {'$avg': "$TAXI_OUT"}}}, {'$sort': {'_id': 1}}])
    print('Month |  Taxi Out Time in Minutes')
    print('----------------------------------')
    for result in results:
        if result['_id'] is not None:
            print("%-6d|%9.2f" % (result['_id'], result['avgTaxiOutTime']))

# Find the average delay due to weather when leaving from a certain airpor
def averageWeatherDelayWhenLeaving(airport):
    results = collection.aggregate([{'$match': {"ORIGIN": airport}}, {'$group': {'_id': "$MONTH", 'average_weather_delay': {'$avg': "$WEATHER_DELAY"}}}, {'$sort': {'_id': 1}}])
    print('Month |  Weather Delay in Minutes')
    print('----------------------------------')
    for result in results:
        if result['_id'] is not None:
            print("%-6d|%9.2f" % (result['_id'], result['average_weather_delay']))

# Lists the months and number of cancelled flights in a given year
def findNumberOfCancelledFlights(month, year):
    print('The number of flights cancelled in {}-{} is {} from a total of {} flights'.format(month, year, collection.find({'MONTH': int(month), 'YEAR': int(year), 'CANCELLED': 1}).count(), collection.find({'MONTH': int(month), 'YEAR': int(year)}).count()))

# Find the distance in miles between two airports
def findDistanceBetweenAirports(airport1, airport2):
    results = collection.aggregate([{'$match': {"ORIGIN": airport1, "DEST": airport2}}, {'$group': {'_id': "$ORIGIN",'distance': { '$avg': "$DISTANCE"}}}])
    for result in results:
            print("The distance between %s and %s is %.0f miles" % (airport1, airport2, result['distance']))

# Find the average delay due to late aircraft given a specific carrier and year
def averageLateAircraftDelayFromCarrier(carrier, year):
    results = collection.aggregate([{'$match': {"CARRIER": carrier, "YEAR": int(year)}}, {'$group': {'_id': "$CARRIER",'late_delay': { '$avg': "$NAS_DELAY"}}}])
    for result in results:
            print("The average delay due to late aircraft from %s in %s is %.2f minutes" % (carrier, year, result['late_delay']))

# Find the month and delay with the longest delay given a year
def findTheMonthWithTheLongestDelayGivenAYear(year):
    results = collection.aggregate([{'$match': {"YEAR": int(year)}}, {'$group':{'_id': "$MONTH",'delay': { '$avg': "$ARR_DELAY" }}}, {'$sort': {'delay': -1}}, {'$limit': 1}])
    for result in results:
        print("The month with the longest delays in %s was month %s at %.2f minutes of delay" % (year, result['_id'], result['delay']))

# Finds the destination state that most flights go to given an origin state
def findMostTraveledStateFromGivenState(state):
    results = collection.aggregate([{'$match': {"ORIGIN_STATE_NM": state}}, {'$group':{'_id': {'origin': "$ORIGIN_STATE_NM", 'dest': "$DEST_STATE_NM"}, 'count': { '$sum': 1}}}, {'$sort': {'count': -1}}, {'$limit': 1}])
    for result in results:
        print("The state with the most flights from %s is %s with a number of %d flights" % (state, result['_id']['dest'], result['count']))

# List the month and the carrier with the most amount of flights cancelled in a given year
def listMonthAndCarrierOfMostCancelledFlights(year):
    results = collection.aggregate([{'$match': {"CANCELLED": 1, "YEAR": int(year)}}, {'$group':{'_id': {'month': "$MONTH", 'carrier': "$CARRIER"}, 'count': { '$sum': 1}}}, {'$sort': {'count': -1}}, {'$group': {'_id':'$_id.month', 'carrier':{'$first': '$_id.carrier'},'count':{'$first': '$count'}}}, {'$sort': {'_id': 1}}])
    print('Month |  Carrier  | Number of Cancellations')
    print('-------------------------------------------')
    for result in results:
        if result['_id'] is not None:
            print("%-6d|%6s   |%13d" % (result['_id'], result['carrier'], result['count'] ))

mainPrompt()