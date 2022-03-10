import express = require("express");

const app = express()
import path = require('path')

require('dotenv').config({path: path.join(__dirname, '../.env')})
const port = process.env.API_PORT
const fileUpload = require('express-fileupload')

import {createConnection, getConnection} from 'typeorm'
import cors = require('cors')
import bodyParser = require('body-parser')

const router = express.Router()
import User from "./entities/User";
import UserController from "./controllers/UserController";
import SessionController from "./controllers/SessionController";
import isAuthenticated from "./middleware/isAuthenticated";
import FileController from "./controllers/FileController";

const main = async () => {

    app.use(bodyParser.json())
    app.use(cors({origin: process.env.FRONTEND_SERVER, credentials: true}))

    //Creating database connection
    await createConnection({
        type: 'mysql',
        host: process.env.DB_HOST as string,
        port: process.env.DB_PORT as any,
        username: process.env.DB_USER as string,
        password: process.env.DB_PWD as string,
        database: process.env.DB_NAME as string,
        synchronize: true,
        entities: [User],
        logging: true,
    })

    //
    // router.put('/user', UserController.create)
    //
    // router.put('/session', SessionController.create)
    // router.get('/session', isAuthenticated, SessionController.show)
    // router.delete('/session', isAuthenticated, SessionController.destroy)



    router.post('/file',fileUpload(),FileController.create)
    router.delete('/file/:uuid',  FileController.destroy)
    router.get('/files/:uuid', FileController.fetchFile)

    app.use('/api', router)
    app.listen(port, () => {
        console.log(`App listening at http://localhost:${port}/api`)
    })
}
main()