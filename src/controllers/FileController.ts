import path = require('path')
import * as fs from "fs";

const FileController = {


    create: async (req: any, res: any) => {

        if (!req.files) {
            return res.status(500)
        }
        const myFile = req.files.file
        const filename = req.body.filename;
        console.log(filename)

        myFile.name = myFile.name.replace(myFile.name, `${filename}`)

        fs.stat(`${process.env.FILES}/${myFile.name}`, function(err, stat) {

            if(err == null) {
                console.log('File exists, Replacing...');

                //  mv() method places the file inside public directory

                myFile.mv(`${process.env.FILES}/${myFile.name}`, function (err: any) {
                    if (err) {
                        console.log(err)
                        return res.status(500)
                    }else{
                        console.log("FIle Successfully saved to SAP directory")
                    }
                })
            } else if(err.code === 'ENOENT') {
                // file does not exist
                //  mv() method places the file inside public directory

                myFile.mv(`${process.env.FILES}/${myFile.name}`, function (err: any) {
                    if (err) {
                        console.log(err)
                        return res.status(500)
                    }else{
                        console.log("FIle Successfully saved to SAP directory")
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

    destroy: async (req: any, res: any) => {

        return res.status(200).send({data: true})

    },

    fetchFile: async (req: any, res: any) => {

        return null;
        // return res.download(`${process.env.FILES}/${filedetails[0].filename}`, filedetails[0].filename.split("-uuid-")[0]);
    },

}

export default FileController