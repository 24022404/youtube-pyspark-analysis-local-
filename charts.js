// ========================================
// CONFIGURATION
// ========================================
const API_BASE_URL = 'http://localhost:5000';
const SOCKET_URL = 'http://localhost:5000';

// ========================================
// WEBSOCKET CONNECTION
// ========================================
let socket;

function initWebSocket() {
    socket = io(SOCKET_URL);
    
    socket.on('connect', () => {
        console.log('✅ WebSocket connected');
        updateConnectionStatus(true);
        socket.emit('request_update');
    });
    
    socket.on('disconnect', () => {
        console.log('❌ WebSocket disconnected');
        updateConnectionStatus(false);
    });
    
    socket.on('stats_update', (data) => {
        updateHeaderStats(data);
    });
    
    socket.on('videos_update', (data) => {
        updateTopVideos(data.videos);
    });
    
    socket.on('realtime_update', (data) => {
        updateRealtimeFeed(data);
    });
    
    socket.on('analytics_update', (data) => {
        console.log('📊 Analytics update received:', data);
        updateEngagementChart(data);
    });
}

function updateConnectionStatus(connected) {
    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('connection-status');
    
    if (connected) {
        statusDot.classList.add('connected');
        statusText.textContent = 'Connected';
    } else {
        statusDot.classList.remove('connected');
        statusText.textContent = 'Disconnected';
    }
}

// ========================================
// CHART INITIALIZATION
// ========================================
let categoryChart, timeChart, engagementChart;

function initCharts() {
    // Category Chart (Pie)
    const categoryCtx = document.getElementById('categoryChart').getContext('2d');
    categoryChart = new Chart(categoryCtx, {
        type: 'doughnut',
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: [
                    '#00d4ff', '#ff00ff', '#00ff00', '#ffff00', '#ff6b6b',
                    '#4ecdc4', '#45b7d1', '#f7b731', '#5f27cd', '#00d2d3'
                ],
                borderWidth: 2,
                borderColor: '#1a1a2e'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#e4e4e4',
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.parsed + ' videos';
                        }
                    }
                }
            }
        }
    });
    
    // Time Chart (Bar)
    const timeCtx = document.getElementById('timeChart').getContext('2d');
    timeChart = new Chart(timeCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Videos',
                data: [],
                backgroundColor: 'rgba(0, 212, 255, 0.6)',
                borderColor: '#00d4ff',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { color: '#a0a0a0' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                },
                x: {
                    ticks: { color: '#a0a0a0' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                }
            },
            plugins: {
                legend: {
                    labels: { color: '#e4e4e4' }
                }
            }
        }
    });
    
    // Engagement Chart (Line)
    const engagementCtx = document.getElementById('engagementChart').getContext('2d');
    engagementChart = new Chart(engagementCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Avg Engagement Rate',
                data: [],
                borderColor: '#ff00ff',
                backgroundColor: 'rgba(255, 0, 255, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { 
                        color: '#a0a0a0',
                        callback: function(value) {
                            return (value * 100).toFixed(2) + '%';
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                },
                x: {
                    ticks: { color: '#a0a0a0' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                }
            },
            plugins: {
                legend: {
                    labels: { color: '#e4e4e4' }
                }
            }
        }
    });
}

// ========================================
// DATA FETCHING
// ========================================
async function fetchStats() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/stats`);
        const data = await response.json();
        updateHeaderStats(data);
    } catch (error) {
        console.error('Error fetching stats:', error);
    }
}

async function fetchCategoryDistribution() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/analytics/category`);
        const data = await response.json();
        
        const labels = data.categories.map(c => c.category);
        const values = data.categories.map(c => c.count);
        
        categoryChart.data.labels = labels;
        categoryChart.data.datasets[0].data = values;
        categoryChart.update();
    } catch (error) {
        console.error('Error fetching categories:', error);
    }
}

async function fetchTimePatterns() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/analytics/time`);
        const data = await response.json();
        
        const labels = data.hourly.map(h => h._id + ':00');
        const values = data.hourly.map(h => h.count);
        
        timeChart.data.labels = labels;
        timeChart.data.datasets[0].data = values;
        timeChart.update();
    } catch (error) {
        console.error('Error fetching time patterns:', error);
    }
}

async function fetchEngagement() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/analytics/engagement`);
        const data = await response.json();
        
        const labels = data.snapshots.map(s => 
            new Date(s.window_start).toLocaleTimeString('en-US', { 
                hour: '2-digit', 
                minute: '2-digit' 
            })
        );
        const values = data.snapshots.map(s => s.avg_engagement);
        
        engagementChart.data.labels = labels.reverse();
        engagementChart.data.datasets[0].data = values.reverse();
        engagementChart.update();
    } catch (error) {
        console.error('Error fetching engagement:', error);
    }
}

async function fetchTopVideos() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/trending/top?limit=10`);
        const data = await response.json();
        updateTopVideos(data.videos);
    } catch (error) {
        console.error('Error fetching top videos:', error);
    }
}

async function fetchPredictions() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/predictions`);
        const data = await response.json();
        updatePredictions(data.predictions);
    } catch (error) {
        console.error('Error fetching predictions:', error);
    }
}

// ========================================
// UI UPDATE FUNCTIONS
// ========================================
function updateHeaderStats(data) {
    document.getElementById('total-videos').textContent = 
        formatNumber(data.total_videos || 0);
    document.getElementById('recent-videos').textContent = 
        formatNumber(data.recent_videos || 0);
    document.getElementById('avg-engagement').textContent = 
        ((data.avg_engagement || 0) * 100).toFixed(2) + '%';
    
    updateLastUpdate();
}

function updateTopVideos(videos) {
    const container = document.getElementById('top-videos');
    
    if (!videos || videos.length === 0) {
        container.innerHTML = '<p style="color: #a0a0a0;">No videos available</p>';
        return;
    }
    
    container.innerHTML = videos.map((video, index) => `
        <div class="video-item">
            <img src="${video.thumbnail_link || 'https://via.placeholder.com/120x67'}" 
                 alt="${video.title}" 
                 class="video-thumbnail">
            <div class="video-info">
                <div class="video-title">${index + 1}. ${video.title}</div>
                <div class="video-meta">
                    ${video.channelTitle} • ${video.category_name}
                </div>
                <div class="video-stats">
                    <div class="stat-item">
                        <span class="emoji">👁️</span>
                        <span>${formatNumber(video.view_count)}</span>
                    </div>
                    <div class="stat-item">
                        <span class="emoji">👍</span>
                        <span>${formatNumber(video.likes)}</span>
                    </div>
                    <div class="stat-item">
                        <span class="emoji">💝</span>
                        <span>${(video.engagement_rate * 100).toFixed(2)}%</span>
                    </div>
                </div>
            </div>
        </div>
    `).join('');
}

function updatePredictions(predictions) {
    const container = document.getElementById('predictions');
    
    if (!predictions || predictions.length === 0) {
        container.innerHTML = '<p style="color: #a0a0a0;">No predictions available</p>';
        return;
    }
    
    container.innerHTML = predictions.map((pred, index) => `
        <div class="prediction-item">
            <div class="prediction-title">${index + 1}. ${pred.title}</div>
            <span class="prediction-score">Score: ${(pred.trending_score * 100).toFixed(1)}%</span>
            <div class="prediction-details">
                <span>📊 ${pred.category_name}</span>
                <span>👁️ Current: ${formatNumber(pred.current_view_count || 0)}</span>
                <span>🔮 Predicted: ${formatNumber(pred.predicted_views || 0)}</span>
                <span>⚡ Velocity: ${formatNumber(pred.view_velocity || 0)}/min</span>
            </div>
        </div>
    `).join('');
}

function updateRealtimeFeed(data) {
    const container = document.getElementById('realtime-feed');
    
    const feedHtml = `
        <div class="feed-item">
            <strong>📊 ${data.videos_count} videos</strong> in last 5 minutes<br>
            👁️ Total views: ${formatNumber(data.total_views)}<br>
            💝 Avg engagement: ${(data.avg_engagement * 100).toFixed(2)}%<br>
            <div style="margin-top: 8px;">
                ${Object.entries(data.categories || {}).map(([cat, count]) => 
                    `<span style="margin-right: 10px;">${cat}: ${count}</span>`
                ).join('')}
            </div>
        </div>
    `;
    
    container.innerHTML = feedHtml + container.innerHTML;
    
    // Keep only last 10 items
    const items = container.querySelectorAll('.feed-item');
    if (items.length > 10) {
        items[items.length - 1].remove();
    }
}

function updateEngagementChart(data) {
    // Add new data point to engagement chart
    if (data.avg_engagement !== undefined) {
        const now = new Date().toLocaleTimeString('en-US', { 
            hour: '2-digit', 
            minute: '2-digit' 
        });
        
        engagementChart.data.labels.push(now);
        engagementChart.data.datasets[0].data.push(data.avg_engagement);
        
        // Keep only last 20 points
        if (engagementChart.data.labels.length > 20) {
            engagementChart.data.labels.shift();
            engagementChart.data.datasets[0].data.shift();
        }
        
        engagementChart.update();
    }
}

function updateLastUpdate() {
    const now = new Date().toLocaleString();
    document.getElementById('last-update').textContent = now;
}

// ========================================
// UTILITY FUNCTIONS
// ========================================
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// ========================================
// INITIALIZATION
// ========================================
async function init() {
    console.log('🚀 Initializing dashboard...');
    
    // Initialize charts
    initCharts();
    
    // Initialize WebSocket
    initWebSocket();
    
    // Fetch initial data
    await fetchStats();
    await fetchCategoryDistribution();
    await fetchTimePatterns();
    await fetchEngagement();
    await fetchTopVideos();
    await fetchPredictions();
    
    console.log('✅ Dashboard initialized');
    
    // Set up periodic updates
    setInterval(async () => {
        await fetchStats();
        await fetchCategoryDistribution();
        await fetchTimePatterns();
        await fetchEngagement();
        await fetchTopVideos();
    }, 30000); // Update every 30 seconds
    
    // Update predictions less frequently
    setInterval(async () => {
        await fetchPredictions();
    }, 300000); // Update every 5 minutes
}

// Start when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
