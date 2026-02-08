/**
 * KPI Routes - Use Case 2
 * Team Member: Harjot Singh
 */

const express = require('express');
const router = express.Router();
const kpiService = require('../services/kpiService');

router.get('/summary', async (req, res) => {
    try {
        const summary = await kpiService.getKPISummary();
        res.json({ success: true, data: summary });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/delays', async (req, res) => {
    try {
        const delays = await kpiService.getDelayStatistics();
        res.json({ success: true, data: delays });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

router.get('/trends', async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 7;
        const trends = await kpiService.getPerformanceTrends(days);
        res.json({ success: true, data: trends });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;