/**
 * KPI Service - Use Case 2
 * Team Member: Harjot Singh
 * 
 * Monitors airport performance metrics and KPIs
 */

const dbManager = require('../config/database');

class KPIService {
    /**
     * Get current KPI summary
     */
    async getKPISummary() {
        try {
            const redis = dbManager.getRedis();
            const db = dbManager.getMongoDB();

            // Get real-time counters from Redis
            const totalFlights = await redis.get('kpi:flights:total') || '0';
            const delayedFlights = await redis.get('kpi:flights:delayed') || '0';
            const onTimeFlights = await redis.get('kpi:flights:ontime') || '0';

            // Calculate from MongoDB for today
            const today = new Date();
            today.setHours(0, 0, 0, 0);

            const flightStats = await db.collection('flight_schedules').aggregate([
                {
                    $match: {
                        'departure.scheduled': {
                            $gte: today.toISOString()
                        }
                    }
                },
                {
                    $group: {
                        _id: null,
                        total: { $sum: 1 },
                        delayed: {
                            $sum: {
                                $cond: [{ $eq: ['$status', 'delayed'] }, 1, 0]
                            }
                        },
                        onTime: {
                            $sum: {
                                $cond: [{ $eq: ['$status', 'active'] }, 1, 0]
                            }
                        },
                        cancelled: {
                            $sum: {
                                $cond: [{ $eq: ['$status', 'cancelled'] }, 1, 0]
                            }
                        }
                    }
                }
            ]).toArray();

            const stats = flightStats[0] || { total: 0, delayed: 0, onTime: 0, cancelled: 0 };

            const onTimePercentage = stats.total > 0
                ? ((stats.onTime / stats.total) * 100).toFixed(2)
                : 0;

            return {
                totalFlights: stats.total,
                delayedFlights: stats.delayed,
                onTimeFlights: stats.onTime,
                cancelledFlights: stats.cancelled,
                onTimePercentage: parseFloat(onTimePercentage),
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Error getting KPI summary:', error);
            throw error;
        }
    }

    /**
     * Get delay statistics
     */
    async getDelayStatistics() {
        try {
            const db = dbManager.getMongoDB();

            const delayStats = await db.collection('flight_schedules').aggregate([
                {
                    $match: {
                        status: 'delayed'
                    }
                },
                {
                    $group: {
                        _id: null,
                        avgDelay: { $avg: '$delayMinutes' },
                        maxDelay: { $max: '$delayMinutes' },
                        minDelay: { $min: '$delayMinutes' },
                        totalDelayed: { $sum: 1 }
                    }
                }
            ]).toArray();

            const stats = delayStats[0] || {
                avgDelay: 0,
                maxDelay: 0,
                minDelay: 0,
                totalDelayed: 0
            };

            return {
                averageDelay: Math.round(stats.avgDelay || 0),
                maximumDelay: stats.maxDelay || 0,
                minimumDelay: stats.minDelay || 0,
                totalDelayedFlights: stats.totalDelayed,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Error getting delay statistics:', error);
            throw error;
        }
    }

    /**
     * Get performance trends
     */
    async getPerformanceTrends(days = 7) {
        try {
            const db = dbManager.getMongoDB();
            const startDate = new Date();
            startDate.setDate(startDate.getDate() - days);

            const trends = await db.collection('flight_history').aggregate([
                {
                    $match: {
                        date: { $gte: startDate }
                    }
                },
                {
                    $group: {
                        _id: {
                            $dateToString: { format: '%Y-%m-%d', date: '$date' }
                        },
                        totalFlights: { $sum: 1 },
                        delayed: {
                            $sum: { $cond: [{ $eq: ['$status', 'delayed'] }, 1, 0] }
                        },
                        onTime: {
                            $sum: { $cond: [{ $ne: ['$status', 'delayed'] }, 1, 0] }
                        },
                        avgDelay: { $avg: '$delayMinutes' }
                    }
                },
                {
                    $sort: { _id: 1 }
                }
            ]).toArray();

            return {
                period: `${days} days`,
                data: trends.map(t => ({
                    date: t._id,
                    totalFlights: t.totalFlights,
                    delayedFlights: t.delayed,
                    onTimeFlights: t.onTime,
                    averageDelay: Math.round(t.avgDelay || 0),
                    onTimePercentage: ((t.onTime / t.totalFlights) * 100).toFixed(2)
                }))
            };
        } catch (error) {
            console.error('Error getting performance trends:', error);
            throw error;
        }
    }

    /**
     * Update KPI counters in Redis
     */
    async updateKPICounters() {
        try {
            const redis = dbManager.getRedis();
            const db = dbManager.getMongoDB();

            // Count flights by status
            const today = new Date();
            today.setHours(0, 0, 0, 0);

            const counts = await db.collection('flight_schedules').aggregate([
                {
                    $match: {
                        'departure.scheduled': { $gte: today.toISOString() }
                    }
                },
                {
                    $group: {
                        _id: '$status',
                        count: { $sum: 1 }
                    }
                }
            ]).toArray();

            let total = 0;
            let delayed = 0;
            let onTime = 0;

            counts.forEach(c => {
                total += c.count;
                if (c._id === 'delayed') delayed = c.count;
                if (c._id === 'active') onTime = c.count;
            });

            // Update Redis counters
            await redis.set('kpi:flights:total', total.toString());
            await redis.set('kpi:flights:delayed', delayed.toString());
            await redis.set('kpi:flights:ontime', onTime.toString());

            console.log(`✅ KPI counters updated: ${total} total, ${delayed} delayed, ${onTime} on-time`);
        } catch (error) {
            console.error('Error updating KPI counters:', error);
        }
    }
}

module.exports = new KPIService();