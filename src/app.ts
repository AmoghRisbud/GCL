import express = require("express");

const app = express()
import path = require('path')

require('dotenv').config({path: path.join(__dirname, '../.env')})
const port = process.env.API_PORT
const fileUpload = require('express-fileupload')

import {createConnection} from 'typeorm'
import cors = require('cors')
import bodyParser = require('body-parser')

const router = express.Router()
import User from "./entities/User";
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



    router.get('/fetchdata/:filename', FileController.fetchFile)
    router.get('/download/:filename', FileController.download)
    router.post('/file',fileUpload(),FileController.create)

    router.delete('/file/:uuid',  FileController.download)


    app.use('/api', router)
    app.listen(port, () => {
        console.log(`App listening at http://localhost:${port}/api`)
    })
}
main()