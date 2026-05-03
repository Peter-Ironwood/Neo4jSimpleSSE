import express from 'express';
import db from './src/db.js';

const app = express();
// SSE endpoint - streams book recommendations
app.get('/stream', async (req, res) => {
    console.log('📡 Client connected to SSE stream');
    // Set SSE headers
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    });

    // Send initial message
    res.write('event: connected\n');
    res.write('data: {"message": "Connected to book recommendation stream"}\n\n');

    // Function to send a random book recommendation
    async function sendRecommendation() {
        try {
            // Query for a random highly-rated book
            const books = await db.query(`
            MATCH (b:Book)<-[:AUTHORED]-(a:Author)
            WHERE b.average_rating > 3.5
            WITH b, collect(a.name) AS authors, rand() AS r
            ORDER BY r
            LIMIT 1
            RETURN b.title AS title, 
                   b.average_rating AS rating,
                   b.ratings_count AS ratingsCount,
                   authors
        `);

            if (books.length > 0) {
                const book = books[0];

                // Send as SSE event
                res.write('event: recommendation\n');
                res.write(`data: ${JSON.stringify({
                    title: book.title,
                    authors: book.authors.slice(0, 3),
                    rating: book.rating,
                    ratingsCount: book.ratingsCount,
                    timestamp: new Date().toISOString()
                })}\n\n`);
            }
        } catch (error) {
            console.error('Query error:', error.message);
        }
    }

    // Send a recommendation every 3 seconds
    const interval = setInterval(sendRecommendation, 3000);

    // Send first recommendation immediately
    sendRecommendation();

    // Clean up on client disconnect
    req.on('close', () => {
        clearInterval(interval);
        console.log('📴 Client disconnected from SSE stream');
    });
});

// Simple HTML client for testing
app.get('/', (req, res) => {
    res.send(` <!DOCTYPE html> <html> <head> <title>Neo4j SSE Book Stream</title> <style> body { font-family: system-ui, -apple-system, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; } h1 { color: white; text-align: center; } .container { background: white; border-radius: 12px; padding: 30px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); } button { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 6px; font-size: 16px; cursor: pointer; margin-bottom: 20px; } button:hover { background: #5a67d8; } button:disabled { background: #cbd5e0; cursor: not-allowed; } .book { padding: 15px; margin: 10px 0; border-left: 4px solid #667eea; background: #f7fafc; border-radius: 4px; animation: slideIn 0.5s ease; } @keyframes slideIn { from { opacity: 0; transform: translateX(-20px); } } .book-title { font-weight: bold; color: #2d3748; font-size: 18px; } .book-meta { color: #718096; font-size: 14px; margin-top: 5px; } #status { text-align: center; padding: 10px; margin-bottom: 20px; border-radius: 6px; } .connected { background: #c6f6d5; color: #22543d; } .disconnected { background: #fed7d7; color: #742a2a; } #books { max-height: 400px; overflow-y: auto; } </style> </head> <body> <h1>📚 Real-time Book Recommendations</h1> <div class="container"> <button id="toggleBtn">Start Streaming</button> <div id="status" class="disconnected">Disconnected</div> <div id="books"></div> </div>
 <script>
            var eventSource = null;
            const toggleBtn = document.getElementById('toggleBtn');
            const statusDiv = document.getElementById('status');
            const booksDiv = document.getElementById('books');
            var bookCount = 0;

            function connect() {
                // Create SSE connection
                eventSource = new EventSource('/stream');
                
                eventSource.addEventListener('connected', (e) => {
                    const data = JSON.parse(e.data);
                    console.log('Connected:', data.message);
                    statusDiv.textContent = 'Connected - Receiving recommendations...';
                    statusDiv.className = 'connected';
                    toggleBtn.textContent = 'Stop Streaming';
                });

                eventSource.addEventListener('recommendation', (e) => {
                    const book = JSON.parse(e.data);
                    displayBook(book);
                });

                eventSource.onerror = (e) => {
                    console.error('SSE error:', e);
                    disconnect();
                };
            }

            function disconnect() {
                if (eventSource) {
                    eventSource.close();
                    eventSource = null;
                }
                statusDiv.textContent = 'Disconnected';
                statusDiv.className = 'disconnected';
                toggleBtn.textContent = 'Start Streaming';
            }

            function displayBook(book) {
                const bookDiv = document.createElement('div');
                bookDiv.className = 'book';
                bookDiv.innerHTML = \`
                    <div class="book-title">\${book.title}</div>
                    <div class="book-meta">
                        ⭐ \${book.rating.toFixed(1)} · 
                        👥 \${book.ratingsCount.toLocaleString()} reviews · 
                        ✍️ \${book.authors.join(', ')}
                    </div>
                \`;
                
                booksDiv.insertBefore(bookDiv, booksDiv.firstChild);
                bookCount++;
                
                // Keep only last 10 books
                while (booksDiv.children.length > 10) {
                    booksDiv.removeChild(booksDiv.lastChild);
                }
            }

            toggleBtn.addEventListener('click', () => {
                if (eventSource) {
                    disconnect();
                } else {
                    connect();
                }
            });
        </script>
    </body>
    </html>
`);
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log('\n🚀 SSE Server Running!');
    console.log(`📺 Open http://localhost:${PORT} in your browser`);
    console.log(`📡 SSE endpoint: http://localhost:${PORT}/stream`);
    console.log(`\n💡 The server will stream a new book recommendation every 3 seconds`);
});