import React, { Fragment, useState } from 'react';
import Button from '@bostongene/frontend-components/Button';
import { Link } from 'react-router-dom';
import Experimental from 'pages/Experimental';
import axios from 'axios'
interface IDrugProps {
  drugId: string;
}

const Drug = ({ drugId }: IDrugProps) => {
  // const [candidate, setCandidates] = useState([]);
  // axios.get(`/drugs-candidates/${drugId}`).then((({data}) => setCandidates(data.candidate)))
  return (
    <div>
      <div>
        <Link to={`/`}>
          <Button className="bg-button--primary">Back to table</Button>
        </Link>
      </div>
      <Experimental
      drugId = {drugId}/>
    </div>
  );
};

export default Drug;
