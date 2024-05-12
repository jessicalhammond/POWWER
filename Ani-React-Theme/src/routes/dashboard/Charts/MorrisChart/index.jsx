import React from 'react';
// import MorrisChart from './MorrisChart';

export default {

  path: '/dashboard/morris',

  async action() {
    const MorrisChart = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./MorrisChart').default), 'MorrisChart');
    });
    return <MorrisChart key={'morrisjs'} />;
  },
};
