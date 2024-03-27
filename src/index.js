// src/index.js

const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');

function performInnerJoin(data,joinData, joinCondition, fields, table) {
    // Logic for INNER JOIN
    // ...
    
    return data.flatMap(mainRow => {
            const innerJoinedData =  joinData
                .filter(joinRow => {
                    const mainValue = mainRow[joinCondition.left.split('.')[1]];
                    const joinValue = joinRow[joinCondition.right.split('.')[1]];
                    return mainValue === joinValue;
                })
            const tableWithConvertedColumnNames = innerJoinedData
                .map(joinRow => {
                    return fields.reduce((acc, field) => {
                        const [tableName, fieldName] = field.split('.');
                        acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                        return acc;
                    }, {});
                });
                return tableWithConvertedColumnNames;
        });
}

function performLeftJoin(data,joinData, joinCondition, fields, table) {
    // Logic for LEFT JOIN
    // ...
    
      const mainData = data.flatMap(mainRow => {
        let arrs = []
        const filteredDataFromRightTableAsPerLeftTable = joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
        if(filteredDataFromRightTableAsPerLeftTable.length>0)
          arrs =   [...arrs,...(filteredDataFromRightTableAsPerLeftTable.map((filteredRow)=>({...mainRow,...filteredRow})))]
        else{
            const rightTableData = Object.keys(joinData[0]);
            let obj = {...mainRow};
            rightTableData.forEach((field)=>{
                obj[field] = null;
            })
             arrs = [...arrs,obj]    
        }
        return arrs;
    });
    
    // Changing all the column name with selected fields
    const leftJoinFinalTable = [...mainData].map((finalItem)=>{
            return fields.reduce((acc,field)=>{
                    const [,fieldName] = field.split('.')
                    acc[field] = finalItem[fieldName]
                    return acc;
            },{})
    })
    return leftJoinFinalTable;


}

function performRightJoin(data,joinData, joinCondition, fields, table) {
    // Logic for RIGHT JOIN
    // ...
    const mainData = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            }).map((rk)=>({...rk,...mainRow}))    
    });
    // Get the left field
    const [,fieldName] = joinCondition.right.split('.');
    // get the joinData table
    const arr = Object.keys(data[0]);
    const remainingRowsInRightTable = joinData.reduce((acc,re)=>{
        const item = mainData.some((yo)=>yo[fieldName] === re[fieldName])
        if(!item){
            let obj = {};
            arr.forEach((yo)=>{
                obj[yo] = null;
            })
             acc.push({...obj,...re})
        }
        return acc;
    },[])
    const rightJoinFinalTable = [...mainData,...remainingRowsInRightTable].map((finalItem)=>{
            return fields.reduce((acc,field)=>{
                    const [,fieldName] = field.split('.')
                    acc[field] = finalItem[fieldName]
                    return acc;
            },{})
    })
    console.log(rightJoinFinalTable)
    return rightJoinFinalTable
}


async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition,joinType } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
        }
        
    }
    // Apply WHERE clause filtering
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => {
            // You can expand this to handle different operators
            return evaluateCondition(row,clause);
        }))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}
// src/index.js
function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;