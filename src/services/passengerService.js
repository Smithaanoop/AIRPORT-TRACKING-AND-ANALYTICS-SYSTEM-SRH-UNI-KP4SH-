const dbManager = require('../config/database');

class PassengerService {
    
    async getFlightInfo(flightNumber) {
        try {
            const db = dbManager.getMongoDB();
            const redis = dbManager.getRedis();

            const schedule = await db.collection('flight_schedules').findOne({
                flightNumber: flightNumber.toUpperCase()
            });

            if (!schedule) {
                return null;
            }

            const liveKey = `aircraft:${flightNumber}:position`;
            const liveData = await redis.hGetAll(liveKey);

            const gateInfo = await this.getGateAssignment(flightNumber);

            let delayMinutes = 0;
            if (schedule.departure.actual && schedule.departure.scheduled) {
                const actual = new Date(schedule.departure.actual);
                const scheduled = new Date(schedule.departure.scheduled);
                delayMinutes = Math.round((actual - scheduled) / 60000);
            }

            return {
                flightNumber: schedule.flightNumber,
                airline: schedule.airline,
                airlineCode: schedule.airlineCode,
                departure: {
                    airport: schedule.departure.airport,
                    iata: schedule.departure.iata,
                    scheduled: schedule.departure.scheduled,
                    estimated: schedule.departure.estimated,
                    actual: schedule.departure.actual,
                    terminal: gateInfo?.terminal || schedule.departure.terminal,
                    gate: gateInfo?.gate || schedule.departure.gate
                },
                arrival: {
                    airport: schedule.arrival.airport,
                    iata: schedule.arrival.iata,
                    scheduled: schedule.arrival.scheduled,
                    estimated: schedule.arrival.estimated,
                    actual: schedule.arrival.actual,
                    terminal: schedule.arrival.terminal,
                    gate: schedule.arrival.gate
                },
                status: this.determineStatus(schedule, liveData),
                delay: delayMinutes,
                aircraft: schedule.aircraft,
                currentPosition: liveData.latitude ? {
                    latitude: parseFloat(liveData.latitude),
                    longitude: parseFloat(liveData.longitude),
                    altitude: parseFloat(liveData.altitude)
                } : null
            };
        } catch (error) {
            console.error('Error getting flight info:', error);
            throw error;
        }
    }

  
    async getGateAssignment(flightNumber) {
        try {
            const driver = dbManager.getNeo4j();
            const session = driver.session();

            const result = await session.run(`
        MATCH (f:Flight {flightNumber: $flightNumber})-[:ASSIGNED_TO]->(g:Gate)-[:BELONGS_TO]->(t:Terminal)
        RETURN g.gateNumber as gate, t.terminalName as terminal
      `, { flightNumber: flightNumber.toUpperCase() });

            await session.close();

            if (result.records.length > 0) {
                return {
                    gate: result.records[0].get('gate'),
                    terminal: result.records[0].get('terminal')
                };
            }

            return null;
        } catch (error) {
            console.error('Error getting gate assignment:', error);
            return null;
        }
    }

    
    determineStatus(schedule, liveData) {
        if (schedule.status === 'cancelled') return 'Cancelled';
        if (schedule.arrival.actual) return 'Landed';
        if (schedule.departure.actual) return 'In Flight';
        if (liveData && liveData.on_ground === 'true') return 'Boarding';
        if (schedule.status === 'delayed') return 'Delayed';
        return 'On Time';
    }

    
    async searchFlights(query) {
        try {
            const db = dbManager.getMongoDB();

            const searchQuery = {
                $or: [
                    { flightNumber: { $regex: query, $options: 'i' } },
                    { airline: { $regex: query, $options: 'i' } },
                    { 'departure.airport': { $regex: query, $options: 'i' } },
                    { 'arrival.airport': { $regex: query, $options: 'i' } }
                ]
            };

            const flights = await db.collection('flight_schedules')
                .find(searchQuery)
                .limit(20)
                .toArray();

            return flights.map(f => ({
                flightNumber: f.flightNumber,
                airline: f.airline,
                departure: f.departure.airport,
                arrival: f.arrival.airport,
                status: f.status,
                scheduledDeparture: f.departure.scheduled
            }));
        } catch (error) {
            console.error('Error searching flights:', error);
            throw error;
        }
    }

    
    async getFlightsByAirline(airlineCode) {
        try {
            const db = dbManager.getMongoDB();

            const flights = await db.collection('flight_schedules')
                .find({ airlineCode: airlineCode.toUpperCase() })
                .sort({ 'departure.scheduled': 1 })
                .limit(50)
                .toArray();

            return flights;
        } catch (error) {
            console.error('Error getting flights by airline:', error);
            throw error;
        }
    }

    
    async getTodayDepartures() {
        try {
            const db = dbManager.getMongoDB();
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            const tomorrow = new Date(today);
            tomorrow.setDate(tomorrow.getDate() + 1);

            const flights = await db.collection('flight_schedules')
                .find({
                    'departure.scheduled': {
                        $gte: today.toISOString(),
                        $lt: tomorrow.toISOString()
                    }
                })
                .sort({ 'departure.scheduled': 1 })
                .toArray();

            return flights;
        } catch (error) {
            console.error('Error getting today departures:', error);
            throw error;
        }
    }
}

module.exports = new PassengerService();
