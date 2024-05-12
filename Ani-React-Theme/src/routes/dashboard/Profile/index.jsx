import React from 'react';
// import Profile from './Profile';

export default {

  path: '/dashboard/profile',

  async action() {
    const Profile = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Profile').default), 'profile');
    });
    return <Profile key={'profile'} />;
  },
};
