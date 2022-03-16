import * as fs from "fs";
import {exec} from "child_process";

const reader = require('xlsx')
const FileController = {

    create: async (req: any, res: any) => {
        if (!req.files) {
            return res.status(500)
        }
        const myFile = req.files.file
        const filename = req.body.filename;
        myFile.name = myFile.name.replace(myFile.name, `${filename}`)

        fs.stat(`${process.env.FILES}/${myFile.name}`, function (err) {
            if (err == null) {
                //  mv() method places the file inside public directory
                myFile.mv(`${process.env.FILES}/${myFile.name}`, function (err: any) {
                    if (err) {
                        console.log(err)
                        return res.status(500)
                    } else {

                        const {exec} = require('child_process');
                        exec('python main.py', (error: any, stdout: any, stderr: string) => {
                            if (error) {
                                console.error(`exec error: ${error}`);
                                return;
                            }
                            console.log(`${stdout}`);
                            if (stderr != "")
                                console.error(`stderr: ${stderr}`);
                        });

                    }
                })
            } else if (err.code === 'ENOENT') {
                // file does not exist
                //  mv() method places the file inside SAP directory
                myFile.mv(`${process.env.FILES}/${myFile.name}`, function (err: any) {
                    if (err) {
                        console.log(err)
                        return res.status(500)
                    } else {

                        const {exec} = require('child_process');
                        exec('python main.py', (error: any, stdout: any, stderr: string) => {
                            if (error) {
                                console.error(`exec error: ${error}`);
                                return;
                            }
                            console.log(`${stdout}`);
                            if (stderr != "")
                                console.error(`stderr: ${stderr}`);
                        });

                    }
                })
            } else {
                console.log('Some other error: ', err.code);
            }
        });
        return res.send({
            name: myFile.name,

        })
    },

    download: async (req: any, res: any) => {
        const filename = req.params.filename;
        fs.stat(`${process.env.COSTSHEET}/${filename}.xlsx`, function (err) {
            if (err == null) {
                return res.download(`${process.env.COSTSHEET}/${filename}.xlsx`);
            } else if (err.code === 'ENOENT') {
                res.send({message: "File could not found"})
            } else {
                res.send({message: `Some error occurred ${err}`})
            }
        });
    },

    fetchFile: async (req: any, res: any) => {
        const filename = req.params.filename;
        let data = <any>[]
        console.log(filename)
        fs.stat(`${process.env.RawCostsheet}/${filename}.xlsx`, function (err) {
            if (err == null) {
                const file = reader.readFile(`${process.env.RawCostsheet}/${filename}.xlsx`)
                const sheets = file.SheetNames
                for (let i = 0; i < sheets.length; i++) {
                    const temp = reader.utils.sheet_to_json(
                        file.Sheets[file.SheetNames[i]], {defval: ""})
                    temp.forEach((resp: any) => {
                        data.push(resp)
                    })
                }
                 console.log(data)


                return res.send({
                    data: data,
                })
            } else if (err.code === 'ENOENT') {
                res.send({message: 'File could not found'})
            } else {
                res.send({message: `Some error occurred ${err}`})

            }
        });
    },
}

export default FileController