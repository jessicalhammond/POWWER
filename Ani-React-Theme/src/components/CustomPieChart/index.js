import React, { Component } from 'react';
import PieChart from '../../vendor/recharts/lib/chart/PieChart';
import Pie from '../../vendor/recharts/lib/polar/Pie';
import { Cell } from '../../vendor/recharts';


const data = [{ name: 'Group A', value: 400 }, { name: 'Group B', value: 300 },
                  { name: 'Group C', value: 300 }, { name: 'Group D', value: 200 }];
const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];


const RADIAN = Math.PI / 180;

const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index }) => { // eslint-disable-line
  const radius = innerRadius + ((outerRadius - innerRadius) * 0.5);
  const x = cx + (radius * Math.cos(-midAngle * RADIAN));
  const y = cy + (radius * Math.sin(-midAngle * RADIAN));

  return (
    <text
      x={x}
      y={y}
      fill="white"
      textAnchor={x > cx ? 'start' : 'end'}
      dominantBaseline="central"
    >
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

class CustomPieChart extends Component { // eslint-disable-line
  render() {
    return (
      <PieChart width={170} height={100} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          labelLine={false}
          outerRadius={50}
          fill="#8884d8"
        >
          {
            data.map((entry, index) => <Cell key={index} fill={COLORS[index % COLORS.length]} />)
          }
        </Pie>
      </PieChart>
    );
  }
}

export default CustomPieChart;
