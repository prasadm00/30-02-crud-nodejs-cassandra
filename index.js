const express = require('express');
const cassandra = require('cassandra-driver');
const cors = require('cors');
const app = express();
app.use(cors());

let authProvider = new cassandra.auth.PlainTextAuthProvider('cassandra', 'cassandra');
let contactPoints = ['127.0.0.1' ];

let localDataCenter = 'datacenter1';

let client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, localDataCenter: localDataCenter, keyspace:'grocery'});


client.connect(err => {
    if (err) {
        console.error('Error connecting to Cassandra:', err);
    } else {
        console.log('Connected to Cassandra');
    }
})

app.use(express.json());

// Create operation
app.post('/create',(req,res)=>{
    const {name, email}=req.body;
    const id = uuidv4();
    const query=  'INSERT INTO demokeyspace.user (id, name, email) VALUES (?, ?, ?)';
    client.execute(query, [id, name, email], (err, result) => {
        if (err) {
            console.error('Error creating user:', err);
            res.status(500).json({ error: 'Error creating user' });
        } else {
            console.log('User created:', id);
            res.status(201).json({ message: 'User created successfully' });
        }
    })
})

// Read all operation
app.get('/read', (req, res) => {
    const query = 'SELECT * FROM demokeyspace.user';
    client.execute(query, { prepare: true }, (err, result) => {
        if (err) {
            console.error('Error reading user:', err);
            res.status(500).json({ error: 'Error reading user' });
        } else {
            let user = result.rows;
            user = user ? user : [];
            res.status(200).json(user);
        }
    });
});
// Read operation
app.get('/read/:id', (req, res) => {
    const id = req.params.id;
    const query = 'SELECT * FROM users WHERE id = ?';
    client.execute(query, [id], { prepare: true }, (err, result) => {
        if (err) {
            console.error('Error reading user:', err);
            res.status(500).json({ error: 'Error reading user' });
        } else {
            const user = result.first();
            res.status(200).json(user);
        }
    });
});
// Update operation
app.put('/update/:id', (req, res) => {
    const id = req.params.id;
    const { name, email } = req.body;
    const query = 'UPDATE demokeyspace.user SET name = ?, email = ? WHERE id = ?';
    client.execute(query, [name, email, id], { prepare: true }, (err, result) => {
        if (err) {
            console.error('Error updating user:', err);
            res.status(500).json({ error: 'Error updating user' });
        } else {
            console.log('User updated:', id);
            res.status(200).json({ message: 'User updated successfully' });
        }
    });
});
// Delete operation
app.delete('/delete/:id', (req, res) => {
    const id = req.params.id;
    const query = 'DELETE FROM demokeyspace.user WHERE id = ?';
    client.execute(query, [id], { prepare: true }, (err, result) => {
        if (err) {
            console.error('Error deleting user:', err);
            res.status(500).json({ error: 'Error deleting user' });
        } else {
            console.log('User deleted:', id);
            res.status(200).json({ message: 'User deleted successfully' });
        }
    });
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});