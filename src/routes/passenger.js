const express = require('express');
const router = express.Router();
const passengerService = require('../services/passengerService');

router.get('/flight/:flightNumber', async (req, res) => {
    try {
        const { flightNumber } = req.params;
        const flight = await passengerService.getFlightInfo(flightNumber);
        if (!flight) {
            return res.status(404).json({ success: false, error: 'Flight not found' });
        }
        res.json({ success: true, data: flight });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/search', async (req, res) => {
    try {
        const { query } = req.query;
        if (!query) {
            return res.status(400).json({ success: false, error: 'Query parameter required' });
        }
        const flights = await passengerService.searchFlights(query);
        res.json({ success: true, count: flights.length, data: flights });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/airline/:airlineCode', async (req, res) => {
    try {
        const { airlineCode } = req.params;
        const flights = await passengerService.getFlightsByAirline(airlineCode);
        res.json({ success: true, count: flights.length, data: flights });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/departures/today', async (req, res) => {
    try {
        const flights = await passengerService.getTodayDepartures();
        res.json({ success: true, count: flights.length, data: flights });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
