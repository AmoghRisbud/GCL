import * as fs from "fs";
const reader = require('xlsx')
const FileController = {


    create: async (req: any, res: any) => {

        if (!req.files) {
            return res.status(500)
        }
        const myFile = req.files.file
        const filename = req.body.filename;
        console.log(filename)

        myFile.name = myFile.name.replace(myFile.name, `${filename}`)

        fs.stat(`${process.env.FILES}/${myFile.name}`, function(err) {

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

    download: async (req: any, res: any) => {
        const filename = req.params.filename;


        fs.stat(`${process.env.COSTSHEET}/${filename}.xlsx`, function(err) {

            if(err == null) {

                return res.download(`${process.env.COSTSHEET}/${filename}.xlsx`);

            } else if(err.code === 'ENOENT') {
                res.send({message:"no file"})

            } else {
                console.log('Some other error: ', err.code);
                res.send({message:"error"})
            }
        });



    },

    fetchFile: async (req: any, res: any) => {


        const filename = req.params.filename;
        console.log(filename)

        let data = <any>[]

        fs.stat(`${process.env.COSTSHEET}/${filename}.xlsx`, function(err) {

            if(err == null) {

                const file = reader.readFile(`${process.env.COSTSHEET}/${filename}.xlsx`)


                const sheets = file.SheetNames
                for(let i = 0; i < sheets.length; i++)
                {
                    const temp = reader.utils.sheet_to_json(
                        file.Sheets[file.SheetNames[i]],{defval:""})
                    temp.forEach((resp:any) => {
                        data.push(resp)
                    })
                }

                // console.log(data)
                return res.send({
                    data: data,
                })
            } else if(err.code === 'ENOENT') {
                console.log("no file")
            } else {
                console.log('Some other error: ', err.code);
            }
        });
    },

}

export default FileController