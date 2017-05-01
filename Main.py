from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.airstats
collection = db['flight']

def mainPrompt():
    print('Welcome! What would you like to do today? Here are your choices:'
          '\n1 - Find the number of flights coming from a specific state'
          '\n2 - Lists the months and the average departure delay experienced in those months'
          '\n3 - Find the average flight time from flights between certain airports'
          '\n4 - Find all airport codes in a certain state')
    print()
    userValue = input('Enter your choice here: ')
    if userValue == '1':
        value = input('What state would you like to see? ')
        numOfFlightsFromState(value)
    if userValue == '2':
        getAverageDepartureDelayPerMonth()
    if userValue == '3':
        origin = input('Origin: ')
        destination = input('Destination: ')
        averageFlightTimeBetweenAirports(origin, destination)
    if userValue == '4':
        state = input('What state would you like to see? ')
        getAirportCodesInState(state)

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
    results = collection.aggregate([ {'$match': {"ORIGIN_STATE_NM": state}}, {'$group': {'_id': { 'origin' : "$ORIGIN", 'city': "$ORIGIN_CITY_NAME"}} }])
    print('Code | City, State')
    print('-------------------------')
    for result in results:
        print("%-5s| %s" % (result['_id']['origin'], result['_id']['city']))

mainPrompt()